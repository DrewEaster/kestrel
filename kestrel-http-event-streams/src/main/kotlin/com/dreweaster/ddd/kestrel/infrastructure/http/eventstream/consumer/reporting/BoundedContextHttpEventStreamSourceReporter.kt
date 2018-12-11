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

    fun startedSavingOffset()

    fun finishedSavingOffset(offset: Long)

    fun finishedSavingOffset(ex: Throwable)

    fun startedHandlingEvent(eventType: String)

    fun finishedHandlingEvent()

    fun finishedHandlingEvent(ex: Throwable)
}
