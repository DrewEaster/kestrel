package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting

interface BoundedContextHttpEventStreamSourceReporter {

    fun createProbe(subscriberName: String): BoundedContextHttpEventStreamSourceProbe
}

interface BoundedContextHttpEventStreamSourceProbe {

    fun startedConsuming()

    fun finishedConsuming()

    fun finishedConsuming(ex: Throwable)

    fun startedFetchingEventStream()

    fun finishedFetchingEventStream(maxOffset: Long)

    fun finishedFetchingEventStream(ex: Throwable)

    fun startedFetchingOffset()

    fun finishedFetchingOffset()

    fun finishedFetchingOffset(ex: Throwable)

    fun startedFetchingMaxOffset()

    fun finishedFetchingMaxOffset(offset: Long)

    fun finishedFetchingMaxOffset(ex: Throwable)

    fun startedSavingOffset()

    fun finishedSavingOffset(offset: Long)

    fun finishedSavingOffset(ex: Throwable)

    fun startedHandlingEvent(eventType: String)

    fun finishedHandlingEvent()

    fun finishedHandlingEvent(ex: Throwable)
}

class ReportingContext(subscriptionName: String, reporters: List<BoundedContextHttpEventStreamSourceReporter>) : BoundedContextHttpEventStreamSourceProbe {

    private val probes: List<BoundedContextHttpEventStreamSourceProbe> = reporters.map { it.createProbe(subscriptionName) }

    override fun startedConsuming() {
        probes.forEach { it.startedConsuming() }
    }

    override fun finishedConsuming() {
        probes.forEach { it.finishedConsuming() }
    }

    override fun finishedConsuming(ex: Throwable) {
        probes.forEach { it.finishedConsuming(ex) }
    }

    override fun startedFetchingEventStream() {
        probes.forEach { it.startedFetchingEventStream() }
    }

    override fun finishedFetchingEventStream(maxOffset: Long) {
        probes.forEach { it.finishedFetchingEventStream(maxOffset)  }
    }

    override fun finishedFetchingEventStream(ex: Throwable) {
        probes.forEach { it.finishedFetchingEventStream(ex) }
    }

    override fun startedFetchingOffset() {
        probes.forEach { it.startedFetchingOffset() }
    }

    override fun finishedFetchingOffset() {
        probes.forEach { it.finishedFetchingOffset() }
    }

    override fun finishedFetchingOffset(ex: Throwable) {
        probes.forEach { it.finishedFetchingOffset(ex) }
    }

    override fun startedFetchingMaxOffset() {
        probes.forEach { it.startedFetchingMaxOffset() }
    }

    override fun finishedFetchingMaxOffset(offset: Long) {
        probes.forEach { it.finishedFetchingMaxOffset(offset) }
    }

    override fun finishedFetchingMaxOffset(ex: Throwable) {
        probes.forEach { it.finishedFetchingMaxOffset(ex) }
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

    override fun startedHandlingEvent(eventType: String) {
        probes.forEach { it.startedHandlingEvent(eventType) }
    }

    override fun finishedHandlingEvent() {
        probes.forEach { it.finishedHandlingEvent() }
    }

    override fun finishedHandlingEvent(ex: Throwable) {
        probes.forEach { it.finishedHandlingEvent(ex) }
    }
}

object ConsoleReporter : BoundedContextHttpEventStreamSourceReporter {

    class ConsoleProbe(private val subscriberName: String) : BoundedContextHttpEventStreamSourceProbe {

        override fun startedConsuming() {

        }

        override fun finishedConsuming() {

        }

        override fun finishedConsuming(ex: Throwable) {

        }

        override fun startedFetchingEventStream() {

        }

        override fun finishedFetchingEventStream(maxOffset: Long) {

        }

        override fun finishedFetchingEventStream(ex: Throwable) {

        }

        override fun startedFetchingOffset() {

        }

        override fun finishedFetchingOffset() {

        }

        override fun finishedFetchingOffset(ex: Throwable) {

        }

        override fun startedFetchingMaxOffset() {

        }

        override fun finishedFetchingMaxOffset(offset: Long) {

        }

        override fun finishedFetchingMaxOffset(ex: Throwable) {

        }

        override fun startedSavingOffset() {

        }

        override fun finishedSavingOffset(offset: Long) {

        }

        override fun finishedSavingOffset(ex: Throwable) {

        }

        override fun startedHandlingEvent(eventType: String) {

        }

        override fun finishedHandlingEvent() {

        }

        override fun finishedHandlingEvent(ex: Throwable) {

        }
    }

    override fun createProbe(subscriberName: String): BoundedContextHttpEventStreamSourceProbe {
        return ConsoleProbe(subscriberName)
    }
}
