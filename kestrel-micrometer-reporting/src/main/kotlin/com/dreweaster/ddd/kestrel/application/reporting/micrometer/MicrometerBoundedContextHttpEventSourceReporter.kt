package com.dreweaster.ddd.kestrel.application.reporting.micrometer

import com.dreweaster.ddd.kestrel.application.BoundedContextName
import com.dreweaster.ddd.kestrel.application.offset.EmptyOffset
import com.dreweaster.ddd.kestrel.application.offset.LastProcessedOffset
import com.dreweaster.ddd.kestrel.application.offset.Offset
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceProbe
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceReporter
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.EventSourcePage
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.SourceEvent
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.atomic.AtomicLong

typealias GaugeName = String
typealias SubscriberName = String

class MicrometerBoundedContextHttpEventSourceReporter(private val registry: MeterRegistry) : BoundedContextHttpEventSourceReporter {

    val boundedContextNameTag = "bounded_context_name"
    val subscriberNameTag = "subscriber_name"
    val hasBacklogTag = "has_backlog"
    val resultTag = "result"
    val eventTypeTag = "event_type"
    val ignoredTag = "ignored"
    val sourceConsumptionTimer = "event_source_consumption"
    val eventProcessingTimer = "event_source_event_processing"
    val eventSourcePageFetchTimer = "event_source_page_fetch"
    val eventSourceOffsetFetchTimer = "event_source_offset_fetch"
    val eventSourceOffsetPersistTimer = "event_source_offset_persist"
    val currentOffsetGauge = "event_source_current_offset"
    val maxOffsetGauge = "event_source_max_offset"

    private val gaugeMap = mutableMapOf<Triple<GaugeName, BoundedContextName, SubscriberName>, AtomicLong>()

    init {

        Timer
            .builder(sourceConsumptionTimer)
            .description("Event source page consumption")
            .tags(boundedContextNameTag, "", subscriberNameTag, "", resultTag, "", hasBacklogTag, "")
            .register(registry)

        Timer
            .builder(eventProcessingTimer)
            .description("Event source event processing")
            .tags(boundedContextNameTag, "", subscriberNameTag, "", eventTypeTag, "", resultTag, "", ignoredTag, "")
            .register(registry)

        Timer
            .builder(eventSourcePageFetchTimer)
            .description("Event source page fetch")
            .tags(boundedContextNameTag, "", subscriberNameTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(eventSourceOffsetFetchTimer)
            .description("Event source offset fetch")
            .tags(boundedContextNameTag, "", subscriberNameTag, "", resultTag, "")
            .register(registry)

        Timer
            .builder(eventSourceOffsetFetchTimer)
            .description("Event source offset persist")
            .tags(boundedContextNameTag, "", subscriberNameTag, "", resultTag, "")
            .register(registry)
    }

    @Synchronized
    private fun getGauge(name: GaugeName, boundedContextName: BoundedContextName, subscriberName: SubscriberName): AtomicLong {
        fun newGauge(): AtomicLong {
            val gaugeValue = AtomicLong(-1)
            registry.gauge(
                name,
                listOf(
                    ImmutableTag(boundedContextNameTag, boundedContextName.name),
                    ImmutableTag(subscriberNameTag, subscriberName)
                ),
                gaugeValue
            )
            gaugeMap[Triple(name, boundedContextName, subscriberName)] = gaugeValue
            return gaugeValue
        }

        return gaugeMap[Triple(name, boundedContextName, subscriberName)] ?: newGauge()
    }

    override fun createProbe(boundedContextName: BoundedContextName, subscriberName: String): BoundedContextHttpEventSourceProbe {
        val currentOffsetGauge = getGauge(currentOffsetGauge, boundedContextName, subscriberName)
        val maxOffsetGauge = getGauge(maxOffsetGauge, boundedContextName, subscriberName)
        return MicrometerCommandHandlingProbe(boundedContextName, subscriberName, currentOffsetGauge, maxOffsetGauge)
    }

    inner class MicrometerCommandHandlingProbe(
            private val boundedContextName: BoundedContextName,
            private val subscriberName: String,
            private val currentOffsetGauge: AtomicLong,
            private val maxOffsetGauge: AtomicLong): BoundedContextHttpEventSourceProbe {

        private var sourceEvent: SourceEvent? = null
        private var sourceConsumptionTimerSample: Timer.Sample? = null
        private var eventProcessingTimerSample: Timer.Sample? = null
        private var eventSourcePageFetchTimerSample: Timer.Sample? = null
        private var eventSourceOffsetFetchTimerSample: Timer.Sample? = null
        private var eventSourceOffsetPersistTimerSample: Timer.Sample? = null

        override fun startedConsuming() {
            if(sourceConsumptionTimerSample == null) sourceConsumptionTimerSample = Timer.start(registry)
        }

        override fun finishedConsuming(hasBacklog: Boolean) {
            sourceConsumptionTimerSample?.stop(registry.timer(sourceConsumptionTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "succeeded",
                hasBacklogTag, "$hasBacklog"
            ))
        }

        override fun finishedConsuming(ex: Throwable) {
            sourceConsumptionTimerSample?.stop(registry.timer(sourceConsumptionTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "failed",
                hasBacklogTag, ""
            ))
        }

        override fun startedFetchingEventSourcePage() {
            if(eventSourcePageFetchTimerSample == null) eventSourcePageFetchTimerSample = Timer.start(registry)
        }

        override fun finishedFetchingEventSourcePage(currentOffset: Offset, page: EventSourcePage) {
            eventSourcePageFetchTimerSample?.stop(registry.timer(eventSourcePageFetchTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "succeeded"
            ))

            maxOffsetGauge.set(page.queryMaxOffset)
        }

        override fun finishedFetchingEventSourcePage(ex: Throwable) {
            eventSourcePageFetchTimerSample?.stop(registry.timer(eventSourcePageFetchTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "failed"
            ))
        }

        override fun startedFetchingOffset() {
            if(eventSourceOffsetFetchTimerSample == null) eventSourceOffsetFetchTimerSample = Timer.start(registry)
        }

        override fun finishedFetchingOffset(offset: Offset) {
            eventSourceOffsetFetchTimerSample?.stop(registry.timer(eventSourceOffsetFetchTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "succeeded"
            ))

            currentOffsetGauge.set(when(offset) {
                is EmptyOffset -> -1
                is LastProcessedOffset -> offset.value
            })
        }

        override fun finishedFetchingOffset(ex: Throwable) {
            eventSourceOffsetFetchTimerSample?.stop(registry.timer(eventSourceOffsetFetchTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "failed"
            ))
        }

        override fun startedSavingOffset() {
            if(eventSourceOffsetPersistTimerSample == null) eventSourceOffsetPersistTimerSample = Timer.start(registry)
        }

        override fun finishedSavingOffset(offset: Long) {
            eventSourceOffsetPersistTimerSample?.stop(registry.timer(eventSourceOffsetPersistTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "succeeded"
            ))

            currentOffsetGauge.set(offset)
        }

        override fun finishedSavingOffset(ex: Throwable) {
            eventSourceOffsetPersistTimerSample?.stop(registry.timer(eventSourceOffsetPersistTimer,
                boundedContextNameTag, boundedContextName.name,
                subscriberNameTag, subscriberName,
                resultTag, "failed"
            ))
        }

        override fun startedHandlingEvent(event: SourceEvent) {
            if(sourceEvent == null) sourceEvent = event
            if(eventProcessingTimerSample == null) eventProcessingTimerSample = Timer.start(registry)
        }

        override fun finishedHandlingEvent(ignored: Boolean) {
            eventProcessingTimerSample?.let { sample -> sourceEvent?.let { e ->
                sample.stop(registry.timer(eventProcessingTimer,
                    boundedContextNameTag, boundedContextName.name,
                    subscriberNameTag, subscriberName,
                    eventTypeTag, e.type,
                    resultTag, "succeeded",
                    ignoredTag, "$ignored"
                ))
            }}
        }

        override fun finishedHandlingEvent(ex: Throwable) {
            eventProcessingTimerSample?.let { sample -> sourceEvent?.let { e ->
                sample.stop(registry.timer(eventProcessingTimer,
                    boundedContextNameTag, boundedContextName.name,
                    subscriberNameTag, subscriberName,
                    eventTypeTag, e.type,
                    resultTag, "failed",
                    ignoredTag, ""
                ))
            }}
        }
    }
}