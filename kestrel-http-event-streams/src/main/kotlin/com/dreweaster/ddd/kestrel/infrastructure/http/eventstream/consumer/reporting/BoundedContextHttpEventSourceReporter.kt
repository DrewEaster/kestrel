package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting

interface BoundedContextHttpEventSourceReporter {

    fun createProbe(subscriberName: String): BoundedContextHttpEventSourceProbe
}

interface BoundedContextHttpEventSourceProbe {

    fun startedConsuming()

    fun finishedConsuming()

    fun finishedConsuming(ex: Throwable)

    fun startedFetchingEventStream()

    fun finishedFetchingEventStream(maxOffset: Long)

    fun finishedFetchingEventStream(ex: Throwable)

    fun startedFetchingOffset()

    fun finishedFetchingOffset()

    fun finishedFetchingOffset(ex: Throwable)

    fun startedSavingOffset()

    fun finishedSavingOffset(offset: Long)

    fun finishedSavingOffset(ex: Throwable)

    fun startedHandlingEvent(eventType: String)

    fun finishedHandlingEvent()

    fun finishedHandlingEvent(ex: Throwable)
}

class ReportingContext(subscriptionName: String, reporters: List<BoundedContextHttpEventSourceReporter>) : BoundedContextHttpEventSourceProbe {

    private val probes: List<BoundedContextHttpEventSourceProbe> = reporters.map { it.createProbe(subscriptionName) }

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

object ConsoleReporter : BoundedContextHttpEventSourceReporter {

    class ConsoleProbe(private val subscriberName: String) : BoundedContextHttpEventSourceProbe {

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

    override fun createProbe(subscriberName: String): BoundedContextHttpEventSourceProbe {
        return ConsoleProbe(subscriberName)
    }
}
