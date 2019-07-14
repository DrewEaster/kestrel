package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.reactivex.Single
import io.vavr.control.Try

class EventSourcedDomainModel(
        private val backend: RxBackend,
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
        return DeduplicatingCommandHandler(aggregateType, aggregateId)
    }

    inner class DeduplicatingCommandHandler<C : DomainCommand, E : DomainEvent, S : AggregateState>(
            private val aggregateType: Aggregate<C, E, S>,
            private val aggregateId: AggregateId) : AggregateRoot<C, E> {

        override fun handleCommandEnvelope(commandEnvelope: CommandEnvelope<C>): Single<CommandHandlingResult<C, E>> {
            return backend.loadEvents(aggregateType, aggregateId)
                .flatMap { recoverAggregate(it) }
                .flatMap { applyCommand(commandEnvelope, it) }
                .flatMap { persistEvents(it.first, it.second) }
                .onErrorReturn(errorHandler(commandEnvelope))
        }

        private fun persistEvents(aggregate: RecoveredAggregate<E, S>, result: CommandHandlingResult<C, E>): Single<CommandHandlingResult<C, E>> {
            return if(result is SuccessResult && result.generatedEvents.isNotEmpty()) {
                backend.saveEvents(
                    aggregateType,
                    aggregateId,
                    CausationId(result.command.commandId.value),
                    result.generatedEvents,
                    aggregate.version,
                    result.command.correlationId
                ).map { result }
            } else Single.just(result)
        }

        private fun applyCommand(commandEnvelope: CommandEnvelope<C>, aggregate: RecoveredAggregate<E, S>): Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>> {
            return when {
                aggregate.isNew -> applyEdenCommand(aggregate, commandEnvelope)
                else -> applyNonEdenCommand(aggregate, commandEnvelope)
            }
        }

        private fun applyEdenCommand(aggregate: RecoveredAggregate<E, S>, commandEnvelope: CommandEnvelope<C>): Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>>{
            if(!aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                val rejectionResult = RejectionResult<C, E>(commandEnvelope, UnsupportedCommandInEdenBehaviour)
                return Single.just(aggregate to (rejectionResult as CommandHandlingResult<C, E>))
            }

            return translateCommandApplicationResult(
                aggregate,
                commandEnvelope,
                aggregateType.blueprint.edenCommandHandler(commandEnvelope.command)
            )
        }

        private fun applyNonEdenCommand(aggregate: RecoveredAggregate<E, S>, commandEnvelope: CommandEnvelope<C>): Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>>  {
            if(aggregate.hasHandledCommandBefore(commandEnvelope.commandId)) {
                // TODO: If command was handled before but was rejected would be good to return the same rejection here
                // TODO: Would require storing special RejectionEvents in the aggregate's event history
                val generatedEvents = aggregate.previousEvents.filter { it.causationId.value == commandEnvelope.commandId.value }.map { it.rawEvent }
                val result = SuccessResult(commandEnvelope, generatedEvents, deduplicated = true)
                return Single.just(aggregate to (result as CommandHandlingResult<C, E>))
            } else {
                if(aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                    if (!aggregateType.blueprint.edenCommandHandler.options(commandEnvelope.command).allowInAllBehaviours) {
                        // Can't issue an eden command once aggregate already exists
                        val rejectionResult = RejectionResult<C, E>(commandEnvelope, AggregateInstanceAlreadyExists)
                        return Single.just(aggregate to rejectionResult)
                    }
                }

                if(!aggregateType.blueprint.commandHandler.canHandle(aggregate.state!!, commandEnvelope.command)) {
                    val rejectionResult = RejectionResult<C, E>(commandEnvelope, UnsupportedCommandInCurrentBehaviour)
                    return Single.just(aggregate to rejectionResult)
                }

                return translateCommandApplicationResult(
                    aggregate,
                    commandEnvelope,
                    aggregateType.blueprint.commandHandler(aggregate.state, commandEnvelope.command)
                )
            }
        }

        private fun translateCommandApplicationResult(aggregate: RecoveredAggregate<E, S>, commandEnvelope: CommandEnvelope<C>, result: Try<List<E>>): Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>> {
            return when(result) {
                is Try.Success -> {
                    val generatedEvents = result.get()
                    Single.just(aggregate to SuccessResult(commandEnvelope, generatedEvents)).doOnSuccess {
                    } as  Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>>
                }
                else -> {
                    Single.just(aggregate to RejectionResult<C, E>(commandEnvelope, result.cause)).doOnSuccess {
                    } as Single<Pair<RecoveredAggregate<E, S>, CommandHandlingResult<C, E>>>
                }
            }
        }

        private fun recoverAggregate(previousEvents: List<PersistedEvent<E>>): Single<RecoveredAggregate<E, S>> {
            return Single.just(previousEvents.fold(
                    RecoveredAggregate<E, S>(
                            version = -1,
                            previousEvents = emptyList(),
                            state = null,
                            builder = commandDeduplicationStrategyFactory.newBuilder())
            ) { acc, e -> acc.copy(
                    version = e.sequenceNumber,
                    previousEvents = acc.previousEvents + e,
                    state = if(acc.state != null) aggregateType.blueprint.eventHandler(acc.state, e.rawEvent) else aggregateType.blueprint.edenEventHandler(e.rawEvent),
                    builder = acc.builder.addEvent(e)
            )})
        }

        private fun errorHandler(commandEnvelope: CommandEnvelope<C>): (Throwable) -> CommandHandlingResult<C, E> = { ex ->
            when(ex) {
                is OptimisticConcurrencyException -> ConcurrentModificationResult(commandEnvelope)
                else -> UnexpectedExceptionResult(commandEnvelope, ex)
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