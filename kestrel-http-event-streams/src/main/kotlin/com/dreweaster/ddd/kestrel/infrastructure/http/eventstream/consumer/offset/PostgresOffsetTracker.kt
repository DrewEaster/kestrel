package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import com.dreweaster.ddd.kestrel.application.UnexpectedNumberOfRowsAffectedInUpdate
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import reactor.core.publisher.Mono

class PostgresOffsetTracker(private val database: Database) : OffsetTracker {

    override fun getOffset(offsetKey: String): Mono<Long?> = database.withContext { ctx ->
        ctx.select("SELECT last_processed_offset FROM event_stream_offsets WHERE name = $1", { it["last_processed_offset"].long }) {
            this["$1"] = offsetKey
        }
    }.collectList().map { it.firstOrNull() }

    override fun saveOffset(offsetKey: String, offset: Long): Mono<Unit> = database.inTransaction { ctx ->
        ctx.update("INSERT INTO event_stream_offsets (name, last_processed_offset) VALUES($1, $2) ON CONFLICT ON CONSTRAINT name_pkey DO UPDATE SET last_processed_offset = $2") {
            this["$1"] = offsetKey
            this["$2"] = offset
        }.flatMap(validateSingleRowAffected)
    }.single()

    private val validateSingleRowAffected = { rowsAffected: Int ->
        when (rowsAffected) {
            1 -> Mono.just(Unit)
            else -> Mono.error(UnexpectedNumberOfRowsAffectedInUpdate(1, rowsAffected))
        }
    }
}