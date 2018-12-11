package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database

class PostgresOffsetManager(private val database: Database) : OffsetManager {

    suspend override fun getOffset(offsetKey: String): Long? {
        return database.withSession { session ->
            val offset = session.select(
                "SELECT last_processed_offset FROM event_stream_offsets WHERE name = :name",
                mapOf("name" to offsetKey)) { row ->
                row.long("last_processed_offset")
            }.firstOrNull()
            offset
        }
    }

    suspend override fun saveOffset(offsetKey: String, offset: Long) {
        val sql =
            """
                INSERT INTO event_stream_offsets (name,last_processed_offset)
                VALUES (:name,:last_processed_offset)
                ON CONFLICT ON CONSTRAINT event_stream_offsets_pkey
                DO UPDATE SET last_processed_offset = :last_processed_offset WHERE event_stream_offsets.name = :name
            """

        database.withSession { it.update(sql, mapOf("name" to offsetKey, "last_processed_offset" to offset)) }
    }
}