package com.dreweaster.ddd.kestrel.application.reporting.micrometer

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CommandEnvelope
import com.dreweaster.ddd.kestrel.application.CommandHandlingProbe
import com.dreweaster.ddd.kestrel.application.CommandHandlingResult
import com.dreweaster.ddd.kestrel.application.ConcurrentModificationResult
import com.dreweaster.ddd.kestrel.application.DomainModelReporter
import com.dreweaster.ddd.kestrel.application.RejectionResult
import com.dreweaster.ddd.kestrel.application.Snapshot
import com.dreweaster.ddd.kestrel.application.SuccessResult
import com.dreweaster.ddd.kestrel.application.UnexpectedExceptionResult
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class MicrometerDomainModelReporter(private val registry: MeterRegistry) : DomainModelReporter {

    val aggregateTypeTag = "aggregate_type"
    val commandTypeTag = "command_type"
    val resultTag = "result"
    val deduplicatedTag = "deduplicated"
    val eventTypeTag = "event_type"
    val commandExecutionTimer = "command_execution"
    val eventsEmittedCounter = "aggregate_events_emitted"
    val aggregateRecoveryTimer = "aggregate_recovery"
    val snapshotRecoveryTimer = "snapshot_recovery"
    val persistedEventsRecoveryTimer = "persisted_events_recovery"
    val applyCommandTimer = "apply_command"
    val persistEventsTimer = "aggregate_persist_events"

    init {
        Timer
            .builder(commandExecutionTimer)
            .description("Command execution")
            .tags(aggregateTypeTag, "", commandTypeTag, "", resultTag, "", deduplicatedTag, "")
            .register(registry)

        Counter
            .builder(eventsEmittedCounter)
            .description("Total aggregate events emitted")
            .tags(aggregateTypeTag, "", eventTypeTag, "")
            .register(registry)

        Timer
            .builder(aggregateRecoveryTimer)
            .description("Aggregate recovery")
            .tags(aggregateTypeTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(snapshotRecoveryTimer)
            .description("Snapshot recovery")
            .tags(aggregateTypeTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(persistedEventsRecoveryTimer)
            .description("Persisted events recovery")
            .tags(aggregateTypeTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(persistEventsTimer)
            .description("Persist events for aggregate")
            .tags(aggregateTypeTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(applyCommandTimer)
            .description("Apply command to aggregate")
            .tags(aggregateTypeTag, "", resultTag, "", deduplicatedTag, "")
            .register(registry)
    }

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) = true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId): CommandHandlingProbe<C, E, S> {
        return MicrometerCommandHandlingProbe(aggregateType)
    }

    inner class MicrometerCommandHandlingProbe<C : DomainCommand, E : DomainEvent, S : AggregateState>(private val aggregateType: Aggregate<C,E,S>) : CommandHandlingProbe<C,E,S> {

        private var command: CommandEnvelope<C>? = null

        private var commandHandlingTimerSample: Timer.Sample? = null

        private var recoveringAggregateTimerSample: Timer.Sample? = null

        private var recoveringSnapshotTimerSample: Timer.Sample? = null

        private var recoveringPersistedEventsTimerSample: Timer.Sample? = null

        private var applyCommandTimerSample: Timer.Sample? = null

        private var persistEventsTimerSample: Timer.Sample? = null

        override fun startedHandling(command: CommandEnvelope<C>) {
            if(this.command == null) this.command = command
            if(commandHandlingTimerSample == null) commandHandlingTimerSample = Timer.start(registry)
        }

        override fun startedRecoveringAggregate() {
            if(recoveringAggregateTimerSample == null) recoveringAggregateTimerSample = Timer.start(registry)
        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?, snapshot: Snapshot<S>?) {
            recoveringAggregateTimerSample?.stop(registry.timer(aggregateRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "succeeded"
            ))
        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            recoveringAggregateTimerSample?.stop(registry.timer(aggregateRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "failed"
            ))
        }

        override fun startedRecoveringSnapshot() {
            if(recoveringSnapshotTimerSample == null) recoveringSnapshotTimerSample = Timer.start(registry)
        }

        override fun finishedRecoveringSnapshot() {
            recoveringSnapshotTimerSample?.stop(registry.timer(snapshotRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "no_snapshot_recovered"
            ))
        }

        override fun finishedRecoveringSnapshot(unexpectedException: Throwable) {
            recoveringSnapshotTimerSample?.stop(registry.timer(snapshotRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "failed"
            ))
        }

        override fun finishedRecoveringSnapshot(state: S, version: Long) {
            recoveringSnapshotTimerSample?.stop(registry.timer(snapshotRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "snapshot_recovered"
            ))
        }

        override fun startedRecoveringPersistedEvents() {
            if(recoveringPersistedEventsTimerSample == null) recoveringPersistedEventsTimerSample = Timer.start(registry)
        }

        override fun finishedRecoveringPersistedEvents() {
            recoveringPersistedEventsTimerSample?.stop(registry.timer(persistedEventsRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "succeeded"
            ))
        }

        override fun finishedRecoveringPersistedEvents(unexpectedException: Throwable) {
            recoveringPersistedEventsTimerSample?.stop(registry.timer(persistedEventsRecoveryTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "failed"
            ))
        }

        override fun startedApplyingCommand() {
            if(applyCommandTimerSample == null) applyCommandTimerSample = Timer.start(registry)
        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            applyCommandTimerSample?.stop(registry.timer(applyCommandTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "accepted",
                deduplicatedTag, "$deduplicated"
            ))
        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            applyCommandTimerSample?.stop(registry.timer(applyCommandTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "rejected",
                deduplicatedTag, "$deduplicated"
            ))
        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            applyCommandTimerSample?.stop(registry.timer(applyCommandTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "failed",
                deduplicatedTag, "false"
            ))
        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
            if(persistEventsTimerSample == null) persistEventsTimerSample = Timer.start(registry)
        }

        override fun finishedPersistingEvents() {
            persistEventsTimerSample?.stop(registry.timer(persistEventsTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "succeeded"
            ))
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {
            persistEventsTimerSample?.stop(registry.timer(persistEventsTimer,
                aggregateTypeTag, aggregateType.blueprint.name,
                resultTag, "failed"
            ))
        }

        override fun finishedHandling(result: CommandHandlingResult<C, E, S>) {

            fun stopTimerWithResult(result: String, deduplicated: Boolean) {
                commandHandlingTimerSample?.let { sample -> command?.let { c ->
                    sample.stop(registry.timer(commandExecutionTimer,
                        aggregateTypeTag, aggregateType.blueprint.name,
                        commandTypeTag, c.command::class.simpleName,
                        resultTag, result,
                        deduplicatedTag, "$deduplicated"
                    ))
                }}
            }

            when(result) {
                is SuccessResult<C,E,S> -> {
                    stopTimerWithResult("succeeded", result.deduplicated)
                    result.generatedEvents.forEach {
                        registry.counter(
                            eventsEmittedCounter,
                            aggregateTypeTag,
                            aggregateType.blueprint.name,
                            eventTypeTag,
                            it::class.simpleName
                        ).increment()
                    }
                }
                is RejectionResult<C,E,S> -> stopTimerWithResult("rejected", result.deduplicated)
                is ConcurrentModificationResult<C,E,S> -> stopTimerWithResult("failed-concurrent-modification", false)
                is UnexpectedExceptionResult<C,E,S> -> stopTimerWithResult("failed-unexpected-exception", false)
            }
        }
    }
}