package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import io.reactivex.Completable
import io.reactivex.Maybe

interface OffsetManager {

    fun getOffset(offsetKey: String): Maybe<Long>

    fun saveOffset(offsetKey: String, offset: Long): Completable
}

object InMemoryOffsetManager : OffsetManager {

    private var offsetsMap: Map<String, Long> = emptyMap()

    override fun getOffset(offsetKey: String) = offsetsMap[offsetKey]?.let { Maybe.just(it) } ?: Maybe.empty()

    override fun saveOffset(offsetKey: String, offset: Long) = Completable.fromRunnable { offsetsMap += offsetKey to offset }
}