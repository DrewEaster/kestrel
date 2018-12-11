package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

interface OffsetManager {

    suspend fun getOffset(offsetKey: String): Long?

    suspend fun saveOffset(offsetKey: String, offset: Long)
}

object InMemoryOffsetManager : OffsetManager {

    private var offsetsMap: Map<String,Long> = emptyMap()

    suspend override fun getOffset(offsetKey: String) = offsetsMap[offsetKey]

    suspend override fun saveOffset(offsetKey: String, offset: Long) {
        offsetsMap += offsetKey to offset
    }
}