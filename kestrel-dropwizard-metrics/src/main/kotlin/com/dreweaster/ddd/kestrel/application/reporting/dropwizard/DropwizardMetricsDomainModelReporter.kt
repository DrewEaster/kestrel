package com.dreweaster.ddd.kestrel.application.reporting.dropwizard

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
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

class DropwizardMetricsDomainModelReporter(private val metricRegistry: MetricRegistry, private val metricNamePrefix: String) : DomainModelReporter {

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) = true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId): CommandHandlingProbe<C, E, S> = DropwizardMetricsCommandHandlingProbe(aggregateType)

    inner class DropwizardMetricsCommandHandlingProbe<C : DomainCommand, E : DomainEvent, S : AggregateState>(private val aggregateType: Aggregate<C,E,S>) : CommandHandlingProbe<C,E,S> {

        private var command: CommandEnvelope<C>? = null

        private var commandHandlingTimerContext: Timer.Context? = null

        private var recoveringAggregateTimerContext: Timer.Context? = null

        private var applyCommandTimerContext: Timer.Context? = null

        private var persistEventsTimerContext: Timer.Context? = null

        override fun startedHandling(command: CommandEnvelope<C>) {
            if(this.command == null) this.command = command
            if(commandHandlingTimerContext == null) commandHandlingTimerContext = metricRegistry.timer(commandSpecificMetricName(command, "execution")).time()
        }

        override fun startedRecoveringAggregate() {
            if(recoveringAggregateTimerContext == null) recoveringAggregateTimerContext = metricRegistry.timer(aggregateTypeSpecificMetricName("recover-aggregate", "execution")).time()
        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?) {
            recoveringAggregateTimerContext?.stop()
            metricRegistry.counter(aggregateTypeSpecificMetricName("recover-aggregate", "success")).inc()
        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            recoveringAggregateTimerContext?.stop()
            metricRegistry.counter(aggregateTypeSpecificMetricName("recover-aggregate", "failure")).inc()
        }

        override fun startedApplyingCommand() {
            if(applyCommandTimerContext == null) applyCommandTimerContext = metricRegistry.timer(aggregateTypeSpecificMetricName("apply-command", "execution")).time()
        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            applyCommandTimerContext?.stop()
            if (deduplicated) {
                metricRegistry.counter(aggregateTypeSpecificMetricName("apply-command", "success", "deduplicated")).inc()
            } else {
                metricRegistry.counter(aggregateTypeSpecificMetricName("apply-command", "success", "processed")).inc()
            }
        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            applyCommandTimerContext?.stop()
            if (deduplicated) {
                metricRegistry.counter(aggregateTypeSpecificMetricName("apply-command", "rejected", "deduplicated")).inc()
            } else {
                metricRegistry.counter(aggregateTypeSpecificMetricName("apply-command", "rejected", "processed")).inc()
            }
        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            applyCommandTimerContext?.stop()
            metricRegistry.counter(aggregateTypeSpecificMetricName("apply-command", "failed")).inc()
        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
            if(persistEventsTimerContext == null) persistEventsTimerContext = metricRegistry.timer(aggregateTypeSpecificMetricName("persist-events", "execution")).time()
        }

        override fun finishedPersistingEvents(persistedEvents: List<PersistedEvent<E>>) {
            persistEventsTimerContext?.stop()
            metricRegistry.counter(aggregateTypeSpecificMetricName("persist-events", "success")).inc()
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {
            persistEventsTimerContext?.stop()
            metricRegistry.counter(aggregateTypeSpecificMetricName("persist-events", "failure")).inc()
        }

        override fun finishedHandling(result: CommandHandlingResult<E>) {
            commandHandlingTimerContext?.stop()
            if(command != null) {
                when(result) {
                    is SuccessResult<E> -> {
                        if(result.deduplicated) {
                            metricRegistry.counter(commandSpecificMetricName(command!!, "result", "success", "deduplicated")).inc()
                        } else {
                            metricRegistry.counter(commandSpecificMetricName(command!!, "result", "success", "processed")).inc()
                            result.generatedEvents.forEach { metricRegistry.counter(eventSpecificMetricName(it)).inc() }
                        }
                    }

                    // TODO: Should record specific rejection error types
                    is RejectionResult<E> -> {
                        if (result.deduplicated) {
                            metricRegistry.counter(commandSpecificMetricName(command!!, "result", "rejection", "deduplicated")).inc()
                        } else {
                            metricRegistry.counter(commandSpecificMetricName(command!!, "result", "rejection", "processed")).inc()
                        }
                    }
                    is ConcurrentModificationResult<E> -> {
                        metricRegistry.counter(commandSpecificMetricName(command!!, "result", "concurrent-modification")).inc()
                    }
                    is UnexpectedExceptionResult<E> -> {
                        metricRegistry.counter(commandSpecificMetricName(command!!, "result", "unexpected-exception")).inc()
                    }
                }
            }
        }

        private fun commandSpecificMetricName(command: CommandEnvelope<C>, vararg names: String) =
            MetricRegistry.name(listOf(metricNamePrefix, aggregateType.blueprint.name, "commands", command.command::class.simpleName).joinToString("."), *names)

        private fun eventSpecificMetricName(event: E, vararg names: String) =
            MetricRegistry.name(listOf(metricNamePrefix, aggregateType.blueprint.name, "events", event::class.simpleName).joinToString("."), *names)

        private fun aggregateTypeSpecificMetricName(vararg names: String) =
            MetricRegistry.name(listOf(metricNamePrefix, aggregateType.blueprint.name, "command-handling").joinToString("."), *names)
    }
}