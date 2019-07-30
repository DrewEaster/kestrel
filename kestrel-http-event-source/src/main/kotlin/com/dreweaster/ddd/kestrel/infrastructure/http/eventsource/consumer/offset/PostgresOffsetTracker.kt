package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.offset

import com.dreweaster.ddd.kestrel.application.UnexpectedNumberOfRowsAffectedInUpdate
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import reactor.core.publisher.Mono

class PostgresOffsetTracker(private val database: Database) : OffsetTracker {

    override fun getOffset(offsetKey: String): Mono<Long?> = database.withContext { ctx ->
        ctx.select("SELECT last_processed_offset FROM event_stream_offsets WHERE name = :name", { it["last_processed_offset"].long }) {
            this["name"] = offsetKey
        }
    }.collectList().map { it.firstOrNull() }

    override fun saveOffset(offsetKey: String, offset: Long): Mono<Unit> = database.inTransaction { ctx ->
        ctx.update("INSERT INTO event_stream_offsets (name, last_processed_offset) VALUES(:name, :last_processed_offset) ON CONFLICT ON CONSTRAINT name_pkey DO UPDATE SET last_processed_offset = :last_processed_offset") {
            this["name"] = offsetKey
            this["last_processed_offset"] = offset
        }.flatMap(validateSingleRowAffected)
    }.single()

    private val validateSingleRowAffected = { rowsAffected: Int ->
        when (rowsAffected) {
            1 -> Mono.just(Unit)
            else -> Mono.error(UnexpectedNumberOfRowsAffectedInUpdate(1, rowsAffected))
        }
    }
}