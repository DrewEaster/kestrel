package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.offset

import reactor.core.publisher.Mono

sealed class EventSourceOffset
data class LastProcessedOffset(val value: Long): EventSourceOffset()
object EmptyOffset: EventSourceOffset()

interface OffsetTracker {

    fun getOffset(offsetKey: String): Mono<out EventSourceOffset>

    fun saveOffset(offsetKey: String, offset: Long): Mono<Void>
}

object InMemoryOffsetTracker : OffsetTracker {

    private var offsetsMap: Map<String, Long> = emptyMap()

    override fun getOffset(offsetKey: String) = offsetsMap[offsetKey]?.let { Mono.just(LastProcessedOffset(it)) } ?: Mono.just(EmptyOffset)

    override fun saveOffset(offsetKey: String, offset: Long) = Mono.fromCallable { offsetsMap += offsetKey to offset }.then()
}