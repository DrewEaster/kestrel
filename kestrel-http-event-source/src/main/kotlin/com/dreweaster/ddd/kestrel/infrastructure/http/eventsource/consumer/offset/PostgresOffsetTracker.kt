package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.offset

import com.dreweaster.ddd.kestrel.application.UnexpectedNumberOfRowsAffectedInUpdate
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class PostgresOffsetTracker(private val database: Database) : OffsetTracker {

    private val LOG = LoggerFactory.getLogger(PostgresOffsetTracker::class.java)

    override fun getOffset(offsetKey: String): Mono<out EventSourceOffset> = database.withContext { ctx ->
        ctx.select("SELECT last_processed_offset FROM event_stream_offsets WHERE name = :name", { LastProcessedOffset(it["last_processed_offset"].long) as EventSourceOffset }) {
            this["name"] = offsetKey
        }
    }.switchIfEmpty(Mono.just(EmptyOffset)).single()

    override fun saveOffset(offsetKey: String, offset: Long): Mono<Void> = database.inTransaction { ctx ->
        ctx.update("INSERT INTO event_stream_offsets (name, last_processed_offset) VALUES(:name, :last_processed_offset) ON CONFLICT ON CONSTRAINT event_stream_offsets_pkey DO UPDATE SET last_processed_offset = :last_processed_offset") {
            this["name"] = offsetKey
            this["last_processed_offset"] = offset
        }.flatMap(validateSingleRowAffected)
    }.then()

    private val validateSingleRowAffected = { rowsAffected: Int ->
        when (rowsAffected) {
            1 -> Mono.empty<Void>()
            else -> Mono.error(UnexpectedNumberOfRowsAffectedInUpdate(1, rowsAffected))
        }
    }
}