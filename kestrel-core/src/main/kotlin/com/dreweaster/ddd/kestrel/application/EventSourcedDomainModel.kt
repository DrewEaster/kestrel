package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.vavr.control.Try
import reactor.core.publisher.Mono

/**
 * TODO: Re-enable DomainModelReporter integration
 */
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
            aggregateId: AggregateId): AggregateRoot<C, E, S> {
        return DeduplicatingCommandHandler(aggregateType, aggregateId)
    }

    inner class DeduplicatingCommandHandler<C : DomainCommand, E : DomainEvent, S : AggregateState>(
            private val aggregateType: Aggregate<C,E,S>,
            private val aggregateId: AggregateId) : AggregateRoot<C, E, S> {

        override fun handleCommandEnvelope(commandEnvelope: CommandEnvelope<C>): Mono<CommandHandlingResult<C, E, S>> {
            val recoverableAggregate = RecoverableAggregate(aggregateId, aggregateType, commandDeduplicationStrategyFactory.newBuilder())
            return backend.loadEvents(aggregateType, aggregateId)
                .reduce(recoverableAggregate) { aggregate, evt -> aggregate.apply(evt)}
                .flatMap { applyCommand(commandEnvelope, it) }
                .flatMap { if(commandEnvelope.dryRun) Mono.just(it.second) else persistEvents(it.first, it.second) }
                .onErrorResume(errorHandler(commandEnvelope))
        }

        private fun persistEvents(aggregate: RecoverableAggregate<C, E, S>, result: CommandHandlingResult<C, E, S>): Mono<out CommandHandlingResult<C, E, S>> {
            return if(result is SuccessResult && result.generatedEvents.isNotEmpty()) {
                backend.saveEvents(
                    aggregateType,
                    aggregateId,
                    CausationId(result.command.commandId.value),
                    result.generatedEvents,
                    aggregate.version,
                    result.command.correlationId
                ).then(Mono.just(result))
            } else Mono.just(result)
        }

        private fun applyCommand(commandEnvelope: CommandEnvelope<C>, aggregate: RecoverableAggregate<C, E, S>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>> {
            return when {
                aggregate.isNew -> applyEdenCommand(aggregate, commandEnvelope)
                else -> applyNonEdenCommand(aggregate, commandEnvelope)
            }
        }

        private fun applyEdenCommand(aggregate: RecoverableAggregate<C, E, S>, commandEnvelope: CommandEnvelope<C>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>{
            if(!aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                val rejectionResult = RejectionResult(aggregateId, aggregateType, commandEnvelope, UnsupportedCommandInEdenBehaviour)
                return Mono.just(aggregate to (rejectionResult as CommandHandlingResult<C, E, S>))
            }

            return translateCommandApplicationResult(
                aggregate,
                commandEnvelope,
                aggregateType.blueprint.edenCommandHandler(commandEnvelope.command)
            )
        }

        private fun applyNonEdenCommand(aggregate: RecoverableAggregate<C, E, S>, commandEnvelope: CommandEnvelope<C>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>  {
            if(aggregate.hasHandledCommandBefore(commandEnvelope.commandId)) {
                // TODO: If command was handled before but was rejected would be good to return the same rejection here
                // TODO: Would require storing special RejectionEvents in the aggregate's event history
                val generatedEvents = aggregate.previousEvents.filter { it.causationId.value == commandEnvelope.commandId.value }.map { it.rawEvent }
                val result = SuccessResult(aggregateId, aggregateType, commandEnvelope, generatedEvents, deduplicated = true)
                return Mono.just(aggregate to (result as CommandHandlingResult<C, E, S>))
            } else {
                if(aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                    if (!aggregateType.blueprint.edenCommandHandler.options(commandEnvelope.command).allowInAllBehaviours) {
                        // Can't issue an eden command once aggregate already exists
                        return Mono.error(AggregateInstanceAlreadyExists)
                    }
                }

                if(!aggregateType.blueprint.commandHandler.canHandle(aggregate.state!!, commandEnvelope.command)) {
                    return Mono.error(UnsupportedCommandInCurrentBehaviour)
                }

                return translateCommandApplicationResult(
                    aggregate,
                    commandEnvelope,
                    aggregateType.blueprint.commandHandler(aggregate.state, commandEnvelope.command)
                )
            }
        }

        private fun translateCommandApplicationResult(aggregate: RecoverableAggregate<C, E, S>, commandEnvelope: CommandEnvelope<C>, result: Try<List<E>>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>> {
            return when(result) {
                is Try.Success -> {
                    val generatedEvents = result.get()
                    Mono.just(aggregate to SuccessResult(aggregateId, aggregateType, commandEnvelope, generatedEvents)) as  Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>
                }
                else -> Mono.just(aggregate to RejectionResult<C, E, S>(aggregateId, aggregateType, commandEnvelope, result.cause)) as Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>
            }
        }

        private fun errorHandler(commandEnvelope: CommandEnvelope<C>): (Throwable) -> Mono<CommandHandlingResult<C, E, S>> = { ex ->
            when(ex) {
                is OptimisticConcurrencyException -> Mono.just(ConcurrentModificationResult(aggregateId, aggregateType, commandEnvelope))
                else -> Mono.just(UnexpectedExceptionResult(aggregateId, aggregateType, commandEnvelope, ex))
            }
        }
    }
}

data class RecoverableAggregate<C: DomainCommand, E: DomainEvent, S: AggregateState>(
    val aggregateId: AggregateId,
    val aggregateType: Aggregate<C, E, S>,
    val builder: CommandDeduplicationStrategyBuilder,
    val version: Long = -1,
    val previousEvents: List<PersistedEvent<E>> = emptyList(),
    val state: S? = null
) {
    val isNew = previousEvents.isEmpty()

    fun apply(evt: PersistedEvent<E>) = copy(
        version = evt.sequenceNumber,
        previousEvents = previousEvents + evt,
        state = if(state != null) aggregateType.blueprint.eventHandler(state, evt.rawEvent) else aggregateType.blueprint.edenEventHandler(evt.rawEvent),
        builder = builder.addEvent(evt)
    )

    fun hasHandledCommandBefore(commandId: CommandId) = builder.build().isDuplicate(commandId = commandId)
}