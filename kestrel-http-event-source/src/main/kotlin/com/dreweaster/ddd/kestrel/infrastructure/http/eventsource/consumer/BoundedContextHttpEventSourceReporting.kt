package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer

import com.dreweaster.ddd.kestrel.application.BoundedContextName
import com.dreweaster.ddd.kestrel.application.offset.Offset

interface BoundedContextHttpEventSourceReporter {

    fun createProbe(boundedContextName: BoundedContextName, subscriberName: String): BoundedContextHttpEventSourceProbe
}

interface BoundedContextHttpEventSourceProbe {

    fun startedConsuming()

    fun finishedConsuming(hasBacklog: Boolean = false)

    fun finishedConsuming(ex: Throwable)

    fun startedFetchingEventSourcePage()

    fun finishedFetchingEventSourcePage(currentOffset: Offset, page: EventSourcePage)

    fun finishedFetchingEventSourcePage(ex: Throwable)

    fun startedFetchingOffset()

    fun finishedFetchingOffset(offset: Offset)

    fun finishedFetchingOffset(ex: Throwable)

    fun startedSavingOffset()

    fun finishedSavingOffset(offset: Long)

    fun finishedSavingOffset(ex: Throwable)

    fun startedHandlingEvent(event: SourceEvent)

    fun finishedHandlingEvent(ignored: Boolean = false)

    fun finishedHandlingEvent(ex: Throwable)
}

class ReportingContext(boundedContextName: BoundedContextName, subscriptionName: String, reporters: List<BoundedContextHttpEventSourceReporter>) : BoundedContextHttpEventSourceProbe {

    private val probes: List<BoundedContextHttpEventSourceProbe> = reporters.map { it.createProbe(boundedContextName, subscriptionName) }

    override fun startedConsuming() {
        probes.forEach { it.startedConsuming() }
    }

    override fun finishedConsuming(hasBacklog: Boolean) {
        probes.forEach { it.finishedConsuming() }
    }

    override fun finishedConsuming(ex: Throwable) {
        probes.forEach { it.finishedConsuming(ex) }
    }

    override fun startedFetchingEventSourcePage() {
        probes.forEach { it.startedFetchingEventSourcePage() }
    }

    override fun finishedFetchingEventSourcePage(currentOffset: Offset, page: EventSourcePage) {
        probes.forEach { it.finishedFetchingEventSourcePage(currentOffset, page)  }
    }

    override fun finishedFetchingEventSourcePage(ex: Throwable) {
        probes.forEach { it.finishedFetchingEventSourcePage(ex) }
    }

    override fun startedFetchingOffset() {
        probes.forEach { it.startedFetchingOffset() }
    }

    override fun finishedFetchingOffset(offset: Offset) {
        probes.forEach { it.finishedFetchingOffset(offset) }
    }

    override fun finishedFetchingOffset(ex: Throwable) {
        probes.forEach { it.finishedFetchingOffset(ex) }
    }

    override fun startedSavingOffset() {
        probes.forEach { it.startedSavingOffset() }
    }

    override fun finishedSavingOffset(offset: Long) {
        probes.forEach { it.finishedSavingOffset(offset) }
    }

    override fun finishedSavingOffset(ex: Throwable) {
        probes.forEach { it.finishedSavingOffset(ex) }
    }

    override fun startedHandlingEvent(event: SourceEvent) {
        probes.forEach { it.startedHandlingEvent(event) }
    }

    override fun finishedHandlingEvent(ignored: Boolean) {
        probes.forEach { it.finishedHandlingEvent() }
    }

    override fun finishedHandlingEvent(ex: Throwable) {
        probes.forEach { it.finishedHandlingEvent(ex) }
    }
}

object ConsoleReporter : BoundedContextHttpEventSourceReporter {

    class ConsoleProbe(private val boundedContextName: BoundedContextName, private val subscriberName: String) : BoundedContextHttpEventSourceProbe {

        override fun startedConsuming() {

        }

        override fun finishedConsuming(hasBacklog: Boolean) {

        }

        override fun finishedConsuming(ex: Throwable) {

        }

        override fun startedFetchingEventSourcePage() {

        }

        override fun finishedFetchingEventSourcePage(currentOffset: Offset, page: EventSourcePage) {

        }

        override fun finishedFetchingEventSourcePage(ex: Throwable) {

        }

        override fun startedFetchingOffset() {

        }

        override fun finishedFetchingOffset(offset: Offset) {

        }

        override fun finishedFetchingOffset(ex: Throwable) {

        }

        override fun startedSavingOffset() {

        }

        override fun finishedSavingOffset(offset: Long) {

        }

        override fun finishedSavingOffset(ex: Throwable) {

        }

        override fun startedHandlingEvent(event: SourceEvent) {

        }

        override fun finishedHandlingEvent(ignored: Boolean) {

        }

        override fun finishedHandlingEvent(ex: Throwable) {

        }
    }

    override fun createProbe(boundedContextName: BoundedContextName, subscriberName: String): BoundedContextHttpEventSourceProbe {
        return ConsoleProbe(boundedContextName, subscriberName)
    }
}
