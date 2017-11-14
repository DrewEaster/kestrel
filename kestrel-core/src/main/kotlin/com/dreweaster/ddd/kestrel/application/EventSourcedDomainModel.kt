package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.vavr.control.Try

class EventSourcedDomainModel(
        private val backend: Backend,
        private val commandDeduplicationStrategyFactory: CommandDeduplicationStrategyFactory) : DomainModel {

    private var reporters: List<DomainModelReporter> = emptyList()

    override fun addReporter(reporter: DomainModelReporter) {
        reporters += reporter
    }

    override fun removeReporter(reporter: DomainModelReporter) {
        reporters -= reporter
    }

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> aggregateRootOf(
            aggregateType: Aggregate<C, E, S>,
            aggregateId: AggregateId): AggregateRoot<C, E> {

        val reportingContext = ReportingContext(aggregateType, aggregateId, reporters)
        return DeduplicatingCommandHandler(aggregateType, aggregateId, reportingContext)
    }

    inner class DeduplicatingCommandHandler<C : DomainCommand, E : DomainEvent, S : AggregateState>(
            private val aggregateType: Aggregate<C, E, S>,
            private val aggregateId: AggregateId,
            private val reportingContext: ReportingContext<C,E,S>) : AggregateRoot<C,E> {

        suspend override fun handle(commandEnvelope: CommandEnvelope<C>): CommandHandlingResult<E> {
            reportingContext.startedHandling(commandEnvelope)

            val result = try {
                val aggregate = recoverAggregate()

                reportingContext.startedApplyingCommand()

                when {
                    aggregate.isNew -> handleEdenCommand(aggregate, commandEnvelope)
                    else -> handleCommand(aggregate, commandEnvelope)
                }
            } catch(ocex: OptimisticConcurrencyException) {
                ConcurrentModificationResult<E>()
            } catch(ex: Throwable) {
                UnexpectedExceptionResult<E>(ex)
            }

            reportingContext.finishedHandling(result)
            return result
        }

        private suspend fun handleEdenCommand(aggregate: RecoveredAggregate<E,S>, commandEnvelope: CommandEnvelope<C>): CommandHandlingResult<E> {
            if(!aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                val rejectionResult = RejectionResult<E>(UnsupportedCommandInEdenBehaviour)
                reportingContext.commandApplicationRejected(rejectionResult.error, rejectionResult.deduplicated)
                return rejectionResult
            }

            return try {
                handleCommandApplicationResult(commandEnvelope, aggregate, aggregateType.blueprint.edenCommandHandler(commandEnvelope.command))
            } catch(ex: Throwable) {
                reportingContext.commandApplicationFailed(ex)
                throw ex
            }
        }

        private suspend fun handleCommand(aggregate: RecoveredAggregate<E, S>, commandEnvelope: CommandEnvelope<C>): CommandHandlingResult<E> {
            if(aggregate.hasHandledCommandBefore(commandEnvelope.commandId)) {
                // TODO: If command was handled before but was rejected would be good to return the same rejection here
                // TODO: Would require storing special RejectionEvents in the aggregate's event history
                val generatedEvents = aggregate.previousEvents.filter { it.causationId.value == commandEnvelope.commandId.value }.map { it.rawEvent }
                val result = SuccessResult(generatedEvents, deduplicated = true)
                reportingContext.commandApplicationAccepted(generatedEvents, deduplicated = true)
                return result
            } else {
                // Can't issue an eden command once aggregate already exists
                if(aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                    val rejectionResult = RejectionResult<E>(AggregateInstanceAlreadyExists)
                    reportingContext.commandApplicationRejected(rejectionResult.error, rejectionResult.deduplicated)
                    return rejectionResult
                }

                if(!aggregateType.blueprint.commandHandler.canHandle(aggregate.state!!, commandEnvelope.command)) {
                    val rejectionResult = RejectionResult<E>(UnsupportedCommandInCurrentBehaviour)
                    reportingContext.commandApplicationRejected(rejectionResult.error, rejectionResult.deduplicated)
                    return rejectionResult
                }

                return try {
                    handleCommandApplicationResult(commandEnvelope, aggregate,  aggregateType.blueprint.commandHandler(aggregate.state, commandEnvelope.command))
                } catch(ex: Throwable) {
                    reportingContext.commandApplicationFailed(ex)
                    throw ex
                }
            }
        }

        private suspend fun handleCommandApplicationResult(
                commandEnvelope: CommandEnvelope<C>,
                aggregate: RecoveredAggregate<E, S>,
                commandApplicationResult: Try<List<E>>): CommandHandlingResult<E> {

            return when(commandApplicationResult) {
                is Try.Success -> {
                    val generatedEvents = commandApplicationResult.get()

                    reportingContext.commandApplicationAccepted(generatedEvents)
                    reportingContext.startedPersistingEvents(generatedEvents, aggregate.version)

                    try {
                        val persistedEvents = backend.saveEvents(
                                aggregateType,
                                aggregateId,
                                CausationId(commandEnvelope.commandId.value),
                                generatedEvents,
                                aggregate.version,
                                commandEnvelope.correlationId)
                        reportingContext.finishedPersistingEvents(persistedEvents)
                    } catch(ex: Throwable) {
                        reportingContext.finishedPersistingEvents(ex)
                        throw ex
                    }

                    SuccessResult(generatedEvents)
                }
                else -> {
                    reportingContext.commandApplicationRejected(commandApplicationResult.cause)
                    RejectionResult(commandApplicationResult.cause)
                }
            }
        }

        // TODO: Check canHandle on event handlers
        private suspend fun recoverAggregate(): RecoveredAggregate<E,S> {
            return try {
                reportingContext.startedRecoveringAggregate()

                val previousEvents = backend.loadEvents(aggregateType, aggregateId)

                val aggregate = previousEvents.fold(
                        RecoveredAggregate<E,S>(
                                version = -1,
                                previousEvents = emptyList(),
                                state = null,
                                builder = commandDeduplicationStrategyFactory.newBuilder())
                ) { acc, e -> acc.copy(
                        version = e.sequenceNumber,
                        previousEvents = acc.previousEvents + e,
                        state = if(acc.state != null) aggregateType.blueprint.eventHandler(acc.state, e.rawEvent) else aggregateType.blueprint.edenEventHandler(e.rawEvent),
                        builder = acc.builder.addEvent(e)
                )}
                reportingContext.finishedRecoveringAggregate(aggregate.rawEvents, aggregate.version, aggregate.state)
                aggregate
            } catch(ex : Throwable) {
                reportingContext.finishedRecoveringAggregate(ex)
                throw ex
            }
        }
    }
}

data class RecoveredAggregate<E: DomainEvent, S: AggregateState>(
        val version: Long,
        val previousEvents: List<PersistedEvent<E>>,
        val state: S?,
        val builder: CommandDeduplicationStrategyBuilder) {

    val isNew = previousEvents.isEmpty()
    val rawEvents = previousEvents.map { it.rawEvent }

    fun hasHandledCommandBefore(commandId: CommandId) = builder.build().isDuplicate(commandId = commandId)
}