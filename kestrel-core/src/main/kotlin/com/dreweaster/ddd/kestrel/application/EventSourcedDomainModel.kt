package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.vavr.control.Try
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface EventSourcingConfiguration {

    fun <E : DomainEvent, S: AggregateState, A: Aggregate<*, E, S>> commandDeduplicationThresholdFor(aggregateType: Aggregate<*, E, S>): Int

    fun <E : DomainEvent, S: AggregateState, A: Aggregate<*, E, S>> snapshotThresholdFor(aggregateType: Aggregate<*, E, S>): Int
}

// TODO: detect corrupt event history (i.e. event sequence numbers not contiguous)
class EventSourcedDomainModel(
        private val backend: Backend,
        private val eventSourcingConfiguration: EventSourcingConfiguration) : DomainModel {

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

        val reportingContext = ReportingContext(aggregateType, aggregateId, reporters)
        return DeduplicatingCommandHandler(aggregateType, aggregateId, reportingContext)
    }

    inner class DeduplicatingCommandHandler<C : DomainCommand, E : DomainEvent, S : AggregateState>(
            private val aggregateType: Aggregate<C,E,S>,
            private val aggregateId: AggregateId,
            private val reportingContext: ReportingContext<C,E,S>) : AggregateRoot<C, E, S> {

        override fun currentState(): Mono<Pair<S, AggregateInstanceVersion>> {
            return recoverAggregate().flatMap {
                if (it.recoveredState == null) Mono.empty()
                else Mono.just(it.recoveredState to it.recoveredVersion)
            }
        }

        override fun stateAt(version: AggregateInstanceVersion): Mono<S> {

            fun applyEvents(state: S?, events: List<PersistedEvent<E>>): S? {
                return events.fold(state) { acc, evt ->
                    if(acc != null) aggregateType.blueprint.eventHandler(acc, evt.rawEvent) else aggregateType.blueprint.edenEventHandler(evt.rawEvent)
                }
            }

            return recoverSnapshot().flatMap { snapshot ->
                when {
                    snapshot.state == null || snapshot.version > version ->
                        recoverEvents(-1, version)
                            .collectList()
                            .switchIfEmpty(Mono.just(emptyList<PersistedEvent<E>>()))
                            .flatMap {
                                when {
                                    it.isEmpty() -> Mono.empty()
                                    it.first().sequenceNumber == 0L ->
                                        when {
                                            it.last().sequenceNumber < version -> Mono.empty()
                                            else -> Mono.just(applyEvents(null, it)) // Don't pass any snapshot here because we need to apply events from eden state
                                        }
                                    else -> Mono.error(EventHistoryCorrupted(aggregateType.blueprint.name, aggregateId.value))
                                }
                            }
                    snapshot.version < version ->
                        recoverEvents(snapshot.version, version)
                            .collectList()
                            .switchIfEmpty(Mono.just(emptyList<PersistedEvent<E>>()))
                            .flatMap {
                                when {
                                    it.isEmpty() -> Mono.empty()
                                    it.last().sequenceNumber < version -> Mono.empty()
                                    else -> Mono.just(applyEvents(snapshot.state, it))
                                }
                            }
                    else -> Mono.just(snapshot.state) // If we get here then snapshot version == requested version, so we can just return the snapshot state
                }
            }
        }

        override fun handleCommandEnvelope(commandEnvelope: CommandEnvelope<C>): Mono<CommandHandlingResult<C, E, S>> {
            reportingContext.startedHandling(commandEnvelope)
            return recoverAggregate().flatMap { recoverableAggregate ->
                applyCommand(commandEnvelope, recoverableAggregate)
                    .flatMap {
                        when {
                            commandEnvelope.dryRun -> Mono.just(it.second)
                            it.second.deduplicated() -> Mono.just(it.second)
                            else -> persistEvents(it.first, it.second)
                        }
                    }
                    .onErrorResume(errorHandler(commandEnvelope, recoverableAggregate))
            }.onErrorResume { Mono.just(UnexpectedExceptionResult(aggregateId, aggregateType, commandEnvelope, null, it))
            }.doOnSuccess { result -> reportingContext.finishedHandling(result) }
        }

        private fun recoverAggregate(): Mono<RecoverableAggregate<C, E, S>> {
            reportingContext.startedRecoveringAggregate()
            return recoverSnapshot().flatMap { snapshot ->
                recoverEvents(afterSequenceNumber = snapshot.version).reduce(RecoverableAggregate(
                    aggregateId = aggregateId,
                    aggregateType =  aggregateType,
                    recoveredSnapshot = snapshot,
                    commandDeduplicationThreshold = eventSourcingConfiguration.commandDeduplicationThresholdFor(aggregateType),
                    snapshotThreshold = eventSourcingConfiguration.snapshotThresholdFor(aggregateType)
                )) { aggregate, evt -> aggregate.apply(evt) }
            }.doOnSuccess { ra -> reportingContext.finishedRecoveringAggregate(ra.eventHistory.map { it.rawEvent }, ra.recoveredVersion, ra.recoveredState, ra.recoveredSnapshot)
            }.doOnError { ex -> reportingContext.finishedRecoveringAggregate(ex) }
        }

        private fun recoverSnapshot(): Mono<Snapshot<S>> {
            reportingContext.startedRecoveringSnapshot()
            return backend.loadSnapshot(aggregateType, aggregateId).defaultIfEmpty(Snapshot(-1, null, emptyList()))
                .doOnSuccess {
                    when(it.state) {
                        null -> reportingContext.finishedRecoveringSnapshot()
                        else -> reportingContext.finishedRecoveringSnapshot(it.state, it.version)
                    }
                }
                .doOnError { reportingContext.finishedRecoveringSnapshot(it) }
        }

        private fun recoverEvents(afterSequenceNumber: Long, toSequenceNumber: Long? = null): Flux<PersistedEvent<E>> {
            reportingContext.startedRecoveringPersistedEvents()
            return backend.loadEvents(aggregateType, aggregateId, afterSequenceNumber, toSequenceNumber)
                .doOnComplete { reportingContext.finishedRecoveringPersistedEvents() }
                .doOnError { reportingContext.finishedRecoveringPersistedEvents(it) }
        }

        private fun persistEvents(aggregate: RecoverableAggregate<C, E, S>, result: CommandHandlingResult<C, E, S>): Mono<out CommandHandlingResult<C, E, S>> {
            return if(result is SuccessResult && result.generatedEvents.isNotEmpty()) {
                reportingContext.startedPersistingEvents(result.generatedEvents, aggregate.recoveredVersion)
                backend.saveEvents(
                    aggregateType = aggregateType,
                    aggregateId = aggregateId,
                    causationId = CausationId(result.command.commandId.value),
                    rawEvents = result.generatedEvents,
                    expectedSequenceNumber = aggregate.recoveredVersion,
                    correlationId = result.command.correlationId,
                    snapshot = aggregate.maybeCreateSnapshot()
                ).doOnComplete { reportingContext.finishedPersistingEvents()
                }.doOnError { reportingContext.finishedPersistingEvents(it) }.then(Mono.just(result))
            } else Mono.just(result)
        }

        private fun applyCommand(commandEnvelope: CommandEnvelope<C>, aggregate: RecoverableAggregate<C, E, S>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>> {
            reportingContext.startedApplyingCommand()
            return when {
                aggregate.isNew -> applyEdenCommand(aggregate, commandEnvelope)
                else -> applyNonEdenCommand(aggregate, commandEnvelope)
            }.doOnSuccess(this::logCommandApplicationResult).doOnError { reportingContext.commandApplicationFailed(it) }
        }

        private fun applyEdenCommand(aggregate: RecoverableAggregate<C, E, S>, commandEnvelope: CommandEnvelope<C>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>{
            if(!aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                val rejectionResult = RejectionResult(aggregateId, aggregateType, commandEnvelope, aggregate.recoveredState, UnsupportedCommandInEdenBehaviour)
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
                // TODO: Would need to apply all events up to most recent event produced by command, and then determine state by applying generated events properly
                val generatedEvents = aggregate.eventHistory.filter { it.causationId.value == commandEnvelope.commandId.value }.map { it.rawEvent }
                val result = SuccessResult(aggregateId, aggregateType, commandEnvelope, null, generatedEvents, deduplicated = true) // FIXME: return correct state
                return Mono.just(aggregate to (result as CommandHandlingResult<C, E, S>))
            } else {
                if(aggregateType.blueprint.edenCommandHandler.canHandle(commandEnvelope.command)) {
                    if (!aggregateType.blueprint.edenCommandHandler.options(commandEnvelope.command).allowInAllBehaviours) {
                        // Can't issue an eden command once aggregate already exists
                        return Mono.error(AggregateInstanceAlreadyExists)
                    }
                }

                if(!aggregateType.blueprint.commandHandler.canHandle(aggregate.recoveredState!!, commandEnvelope.command)) {
                    return Mono.error(UnsupportedCommandInCurrentBehaviour)
                }

                return translateCommandApplicationResult(
                    aggregate,
                    commandEnvelope,
                    aggregateType.blueprint.commandHandler(aggregate.recoveredState, commandEnvelope.command)
                )
            }
        }

        private fun translateCommandApplicationResult(aggregate: RecoverableAggregate<C, E, S>, commandEnvelope: CommandEnvelope<C>, result: Try<List<E>>): Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>> {
            return when(result) {
                is Try.Success -> {
                    val generatedEvents = result.get()
                    // TODO: Handle UnsupportedEventInCurrentBehaviour correctly
                    val updatedState = generatedEvents.fold(aggregate.recoveredState) { acc, evt -> if(acc != null) aggregateType.blueprint.eventHandler(acc, evt) else aggregateType.blueprint.edenEventHandler(evt) }
                    Mono.just(aggregate to SuccessResult(aggregateId, aggregateType, commandEnvelope, updatedState, generatedEvents)) as  Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>
                }
                else -> Mono.just(aggregate to RejectionResult<C, E, S>(aggregateId, aggregateType, commandEnvelope, aggregate.recoveredState, result.cause)) as Mono<Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>>
            }
        }

        private fun logCommandApplicationResult(result: Pair<RecoverableAggregate<C, E, S>, CommandHandlingResult<C, E, S>>) {
            when(result.second) {
                is SuccessResult<C,E,S> -> {
                    val successResult = result.second as SuccessResult<C,E,S>
                    reportingContext.commandApplicationAccepted(successResult.generatedEvents, successResult.deduplicated)
                }
                is RejectionResult<C,E,S> -> {
                    val rejectionResult = result.second as RejectionResult<C,E,S>
                    reportingContext.commandApplicationRejected(rejectionResult.error, rejectionResult.deduplicated)
                }
                is ConcurrentModificationResult<C,E,S> -> {
                    reportingContext.commandApplicationFailed(OptimisticConcurrencyException)
                }
                is UnexpectedExceptionResult<C,E,S> -> {
                    val unexpectedExceptionResult = result.second as UnexpectedExceptionResult<C,E,S>
                    reportingContext.commandApplicationFailed(unexpectedExceptionResult.ex)
                }
            }
        }

        private fun errorHandler(commandEnvelope: CommandEnvelope<C>, aggregate: RecoverableAggregate<C, E, S>): (Throwable) -> Mono<CommandHandlingResult<C, E, S>> = { ex ->
            when(ex) {
                is OptimisticConcurrencyException -> Mono.just(ConcurrentModificationResult(aggregateId, aggregateType, commandEnvelope, aggregate.recoveredState))
                else -> Mono.just(UnexpectedExceptionResult(aggregateId, aggregateType, commandEnvelope, aggregate.recoveredState, ex))
            }
        }
    }
}

data class RecoverableAggregate<C: DomainCommand, E: DomainEvent, S: AggregateState>(
    val aggregateId: AggregateId,
    val aggregateType: Aggregate<C, E, S>,
    val snapshotThreshold: Int,
    val commandDeduplicationThreshold: Int,
    val eventHistory: List<PersistedEvent<E>> = emptyList(),
    val recoveredSnapshot: Snapshot<S>,
    val recoveredVersion: Long = recoveredSnapshot.version,
    val causationIdHistory: LinkedHashSet<CausationId> = linkedSetOf<CausationId>().apply { addAll(recoveredSnapshot.causationIdHistory) },
    val recoveredState: S? = recoveredSnapshot.state,
    val allStates: Map<Long, S> = recoveredSnapshot.state?.let { mapOf(recoveredSnapshot.version to it) } ?: emptyMap()
) {
    val isNew = eventHistory.isEmpty()

    fun apply(evt: PersistedEvent<E>): RecoverableAggregate<C,E,S> {
        val newState = if(recoveredState != null) aggregateType.blueprint.eventHandler(recoveredState, evt.rawEvent) else aggregateType.blueprint.edenEventHandler(evt.rawEvent)
        val newVersion = evt.sequenceNumber
        return copy(
            recoveredVersion = newVersion,
            eventHistory =  if(eventHistory.size < snapshotThreshold) eventHistory + evt else listOf(evt),
            causationIdHistory = (causationIdHistory + evt.causationId) as LinkedHashSet<CausationId>,
            recoveredState = newState,
            recoveredSnapshot = maybeCreateSnapshot() ?: recoveredSnapshot,
            allStates = allStates + (newVersion to newState)
        )
    }

    fun hasHandledCommandBefore(commandId: CommandId) =
        causationIdHistory.toList().takeLast(commandDeduplicationThreshold).contains(CausationId(commandId.value))

    // Create snapshot based on aggregate as it was recovered, not based on its state post command handling
    fun maybeCreateSnapshot(): Snapshot<S>? {
        return if(recoveredState != null && eventHistory.size >= snapshotThreshold) {
            return Snapshot(
                state = recoveredState,
                version = recoveredVersion,
                causationIdHistory = causationIdHistory.toList().takeLast(commandDeduplicationThreshold))
        } else null
    }
}