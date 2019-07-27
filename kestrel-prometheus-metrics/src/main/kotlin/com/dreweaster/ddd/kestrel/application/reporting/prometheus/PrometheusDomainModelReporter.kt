package com.dreweaster.ddd.kestrel.application.reporting.prometheus

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CommandEnvelope
import com.dreweaster.ddd.kestrel.application.CommandHandlingResult
import com.dreweaster.ddd.kestrel.application.ConcurrentModificationResult
import com.dreweaster.ddd.kestrel.application.RejectionResult
import com.dreweaster.ddd.kestrel.application.SuccessResult
import com.dreweaster.ddd.kestrel.application.UnexpectedExceptionResult
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.prometheus.client.Counter
import io.prometheus.client.Summary

class PrometheusDomainModelReporter: DomainModelReporter {

    companion object {

        val commandExecutionLatency = Summary.build()
                .name("aggregate_command_execution_latency_seconds")
                .help("Aggregate command execution latency in seconds.")
                .labelNames("aggregate_type", "command_type")
                .register()

        val commandExecution = Counter.build()
                .name("aggregate_command_execution_total")
                .help("Total aggregate commands executed")
                .labelNames("aggregate_type", "command_type", "result", "deduplicated")
                .register()

        val eventsEmitted =  Counter.build()
                .name("aggregate_events_emitted_total")
                .help("Total aggregate events emitted")
                .labelNames("aggregate_type", "event_type")
                .register()

        val aggregateRecoveryLatency = Summary.build()
                .name("aggregate_recovery_latency_seconds")
                .help("Aggregate recovery latency in seconds.")
                .labelNames("aggregate_type")
                .register()

        val aggregateRecovery = Counter.build()
                .name("aggregate_recovery_total")
                .help("Total aggregates recovered")
                .labelNames("aggregate_type", "result")
                .register()

        val applyCommandLatency = Summary.build()
                .name("aggregate_apply_command_latency_seconds")
                .help("Apply command to aggregate latency in seconds.")
                .labelNames("aggregate_type")
                .register()

        val applyCommand = Counter.build()
                .name("aggregate_apply_command_total")
                .help("Total aggregate commands applied")
                .labelNames("aggregate_type", "result", "deduplicated")
                .register()

        val persistEventsLatency = Summary.build()
                .name("aggregate_persist_events_latency_seconds")
                .help("Persist events for aggregate latency in seconds.")
                .labelNames("aggregate_type")
                .register()

        val persistEvents = Counter.build()
                .name("aggregate_persist_events_total")
                .help("Total calls to persist events for aggregate")
                .labelNames("aggregate_type", "result")
                .register()
    }

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) = true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId): CommandHandlingProbe<C, E, S> = PrometheusCommandHandlingProbe(aggregateType)

    inner class PrometheusCommandHandlingProbe<C : DomainCommand, E : DomainEvent, S : AggregateState>(private val aggregateType: Aggregate<C,E,S>) : CommandHandlingProbe<C,E,S> {

        private var command: CommandEnvelope<C>? = null

        private var commandHandlingTimerContext: Summary.Timer? = null

        private var recoveringAggregateTimerContext: Summary.Timer? = null

        private var applyCommandTimerContext: Summary.Timer? = null

        private var persistEventsTimerContext: Summary.Timer? = null

        override fun startedHandling(command: CommandEnvelope<C>) {
            if(this.command == null) this.command = command
            if(commandHandlingTimerContext == null) commandHandlingTimerContext = commandExecutionLatency.labels(aggregateType.blueprint.name, command.command::class.simpleName).startTimer()
        }

        override fun startedRecoveringAggregate() {
            if(recoveringAggregateTimerContext == null) recoveringAggregateTimerContext = aggregateRecoveryLatency.labels(aggregateType.blueprint.name).startTimer()
        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?) {
            recoveringAggregateTimerContext?.observeDuration()
            aggregateRecovery.labels(aggregateType.blueprint.name, "success").inc()
        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            recoveringAggregateTimerContext?.observeDuration()
            aggregateRecovery.labels(aggregateType.blueprint.name, "failure").inc()
        }

        override fun startedApplyingCommand() {
            if(applyCommandTimerContext == null) applyCommandTimerContext = applyCommandLatency.labels(aggregateType.blueprint.name).startTimer()
        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            applyCommandTimerContext?.observeDuration()
            applyCommand.labels(aggregateType.blueprint.name, "accepted", deduplicated.toString()).inc()
        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            applyCommandTimerContext?.observeDuration()
            applyCommand.labels(aggregateType.blueprint.name, "rejected", deduplicated.toString()).inc()
        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            applyCommandTimerContext?.observeDuration()
            applyCommand.labels(aggregateType.blueprint.name, "failed", "false").inc()
        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
            if(persistEventsTimerContext == null) persistEventsTimerContext = persistEventsLatency.labels(aggregateType.blueprint.name).startTimer()
        }

        override fun finishedPersistingEvents(persistedEvents: List<PersistedEvent<E>>) {
            persistEventsTimerContext?.observeDuration()
            persistEvents.labels(aggregateType.blueprint.name, "success").inc()
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {
            persistEventsTimerContext?.observeDuration()
            persistEvents.labels(aggregateType.blueprint.name, "failure").inc()
        }

        override fun finishedHandling(result: CommandHandlingResult<C, E, S>) {
            commandHandlingTimerContext?.observeDuration()
            if(command != null) {
                when(result) {
                    is SuccessResult<C, E, S> -> {
                        commandExecution.labels(aggregateType.blueprint.name, command!!.command::class.simpleName, "accepted", result.deduplicated.toString()).inc()
                        result.generatedEvents.forEach { eventsEmitted.labels(aggregateType.blueprint.name, it::class.simpleName).inc() }
                    }

                    // TODO: Should record specific rejection error types
                    is RejectionResult<C, E, S> -> {
                        commandExecution.labels(aggregateType.blueprint.name, command!!.command::class.simpleName, "rejected", result.deduplicated.toString()).inc()
                    }
                    is ConcurrentModificationResult<C, E, S> -> {
                        commandExecution.labels(aggregateType.blueprint.name, command!!.command::class.simpleName, "failed-concurrent-modification", "false").inc()
                    }
                    is UnexpectedExceptionResult<C, E, S> -> {
                        commandExecution.labels(aggregateType.blueprint.name, command!!.command::class.simpleName, "failed-unexpected-exception", "false").inc()
                    }
                }
            }
        }
    }
}