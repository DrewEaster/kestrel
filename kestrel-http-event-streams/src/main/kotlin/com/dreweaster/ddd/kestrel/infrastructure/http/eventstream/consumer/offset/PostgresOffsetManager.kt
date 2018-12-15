package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset

import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.upsert
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.select
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.*
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetManager.Offsets.primaryKeyConstraintConflictTarget

class PostgresOffsetManager(private val database: Database) : OffsetManager {

    object Offsets : Table("event_stream_offsets") {
        val name = varchar("name", 100).primaryKey()
        val lastProcessedOffset = long("last_processed_offset")
        val primaryKeyConstraintConflictTarget = primaryKeyConstraintConflictTarget(name)
    }

    override suspend fun getOffset(offsetKey: String) = database.transaction {
        Offsets.slice(Offsets.lastProcessedOffset).select { Offsets.name eq offsetKey }.map { row -> row[Offsets.lastProcessedOffset] }.firstOrNull()
    }

    override suspend fun saveOffset(offsetKey: String, offset: Long) {
        database.transaction { tx ->
            tx.assert(rowsAffected = 1) {
                Offsets.upsert(primaryKeyConstraintConflictTarget, { Offsets.name eq offsetKey }) {
                    it[name] = offsetKey
                    it[lastProcessedOffset] = offset
                }
            }
        }
    }
}