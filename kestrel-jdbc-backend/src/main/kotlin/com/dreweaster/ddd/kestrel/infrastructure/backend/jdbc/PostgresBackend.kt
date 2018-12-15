package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import org.jetbrains.exposed.sql.*
import java.time.Instant
import kotlin.reflect.KClass

class PostgresBackend(
        private val db: Database,
        private val mapper: EventPayloadMapper,
        private val readModels: List<SynchronousJdbcReadModel>) : Backend {

    object DomainEvents : Table("domain_event") {
        val globalOffset  = long("global_offset").primaryKey().autoIncrement()
        val id = varchar("event_id", 72)
        val aggregateId = varchar("aggregate_id", 72)
        val aggregateType = varchar("aggregate_type", 72)
        val tag = varchar("tag", 100)
        val causationId = varchar("causation_id", 72)
        val correlationId = varchar("correlation_id", 72).nullable()
        val type = varchar("event_type", 255)
        val version = integer("event_version")
        val payload = text("event_payload")
        val timestamp = instant("event_timestamp")
        val sequenceNumber = long("sequence_number")
    }

    object AggregateRoots : Table("aggregate_root") {
        val id = varchar("aggregate_id", 72).primaryKey(0)
        val type = varchar("aggregate_type", 255).primaryKey(1)
        val version = long("aggregate_version")
        val primaryKeyConstraintConflictTarget = primaryKeyConstraintConflictTarget(id, type)
    }

    override suspend fun <E : DomainEvent> loadEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId): List<PersistedEvent<E>> {
        return db.transaction {
            DomainEvents.select {
                (DomainEvents.aggregateId eq aggregateId.value) and
                (DomainEvents.aggregateType eq aggregateType.blueprint.name) and
                (DomainEvents.sequenceNumber greater -1L)
            }.map { row -> rowToPersistedEvent(aggregateType, row) }
        }
    }

    override suspend fun <E : DomainEvent> loadEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId, afterSequenceNumber: Long): List<PersistedEvent<E>> {
        return db.transaction {
            DomainEvents.select {
                (DomainEvents.aggregateId eq aggregateId.value) and
                (DomainEvents.aggregateType eq aggregateType.blueprint.name) and
                (DomainEvents.sequenceNumber greater afterSequenceNumber)
            }.map { row -> rowToPersistedEvent(aggregateType, row) }
        }
    }

    override suspend fun <E : DomainEvent> loadEventStream(tag: DomainEventTag, afterOffset: Long, batchSize: Int): EventStream {
        return db.transaction {
            val maxExpr = DomainEvents.globalOffset.max()
            val maxOffset = DomainEvents.slice(maxExpr).select {
                (DomainEvents.tag eq tag.value) and
                (DomainEvents.globalOffset greater afterOffset)
            }.firstOrNull()?.let { row -> row[maxExpr] } ?: -1L

            val events = DomainEvents.select {
                (DomainEvents.tag eq tag.value) and
                (DomainEvents.globalOffset greater afterOffset)
            }.orderBy(DomainEvents.globalOffset).limit(batchSize).map { row ->
                rowToStreamEvent<E>(tag, row)
            }

            EventStream(
                events = events,
                tag = tag,
                batchSize = batchSize,
                startOffset = events.firstOrNull()?.offset,
                endOffset = events.lastOrNull()?.offset,
                maxOffset = if(maxOffset == -1L) events.lastOrNull()?.offset ?: -1L else maxOffset
            )
        }
    }

    override suspend fun <E : DomainEvent> loadEventStream(tag: DomainEventTag, afterInstant: Instant, batchSize: Int): EventStream {
        return db.transaction {
            val maxExpr = DomainEvents.globalOffset.max()
            val maxOffset = DomainEvents.slice(maxExpr).select {
                (DomainEvents.tag eq tag.value) and
                (DomainEvents.timestamp greater afterInstant)
            }.firstOrNull()?.let { row -> row[maxExpr] } ?: -1L

            val events = DomainEvents.select {
                DomainEvents.tag eq tag.value
                DomainEvents.timestamp greater afterInstant
            }.orderBy(DomainEvents.globalOffset).limit(batchSize).map { row ->
                rowToStreamEvent<E>(tag, row)
            }

            EventStream(
                events = events,
                tag = tag,
                batchSize = batchSize,
                startOffset = events.firstOrNull()?.offset,
                endOffset = events.lastOrNull()?.offset,
                maxOffset = if(maxOffset == -1L) events.lastOrNull()?.offset ?: -1L else maxOffset
            )
        }
    }

    override suspend fun <E : DomainEvent> saveEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): List<PersistedEvent<E>> {
        val saveableEvents = rawEvents.fold(Pair(expectedSequenceNumber + 1, emptyList<SaveableEvent<E>>())) { acc, e ->
            Pair(acc.first + 1, acc.second + SaveableEvent(
                id = EventId(),
                aggregateId = aggregateId,
                aggregateType = aggregateType,
                causationId = causationId,
                correlationId = correlationId,
                eventType = e::class as KClass<E>,
                rawEvent = e,
                serialisationResult = mapper.serialiseEvent(e),
                timestamp = Instant.now(),
                sequenceNumber = acc.first
            ))
        }

        val persistedEvents = saveableEvents.second.map { it.toPersistedEvent() }

        db.transaction { tx ->

            DomainEvents.batchInsert(saveableEvents.second) { event ->
                this[DomainEvents.id] = event.id.value
                this[DomainEvents.aggregateId] = event.aggregateId.value
                this[DomainEvents.aggregateType] = aggregateType.blueprint.name
                this[DomainEvents.tag] = event.rawEvent.tag.value
                this[DomainEvents.causationId] = event.causationId.value
                this[DomainEvents.correlationId] = event.correlationId?.value
                this[DomainEvents.type] = event.eventType.qualifiedName!!
                this[DomainEvents.version] = event.serialisationResult.version
                this[DomainEvents.payload] = event.serialisationResult.payload
                this[DomainEvents.timestamp] = event.timestamp
                this[DomainEvents.sequenceNumber] = event.sequenceNumber
            }

            val saveAggregateRowsAffected = AggregateRoots.upsert(AggregateRoots.primaryKeyConstraintConflictTarget, { AggregateRoots.version eq expectedSequenceNumber }) {
                it[id] = aggregateId.value
                it[type] = aggregateType.blueprint.name
                it[version] = saveableEvents.second.last().sequenceNumber
            }

            if(saveAggregateRowsAffected == 0) throw OptimisticConcurrencyException

            // Update synchronous read models
            readModels.forEach {
                val projection = it.update
                if(projection.aggregateType == aggregateType::class) {
                    persistedEvents.forEach { e ->
                        projection.handleEvent(tx, e as PersistedEvent<DomainEvent>)
                    }
                }
            }
        }

        return persistedEvents
    }

    private fun <E: DomainEvent> rowToPersistedEvent(aggregateType: Aggregate<*,E,*>, row: ResultRow): PersistedEvent<E> {
        val rawEvent = mapper.deserialiseEvent<E>(
            row[DomainEvents.payload],
            row[DomainEvents.type],
            row[DomainEvents.version]
        )

        return PersistedEvent(
            id = EventId(row[DomainEvents.id]),
            aggregateId = AggregateId(row[DomainEvents.aggregateId]),
            aggregateType = aggregateType,
            causationId = CausationId(row[DomainEvents.causationId]),
            correlationId = row[DomainEvents.correlationId]?.let { CorrelationId(it) },
            eventType = rawEvent::class as KClass<E>,
            eventVersion = row[DomainEvents.version],
            rawEvent = rawEvent,
            timestamp = row[DomainEvents.timestamp],
            sequenceNumber = row[DomainEvents.sequenceNumber]
        )
    }

    private fun <E: DomainEvent> rowToStreamEvent(tag: DomainEventTag, row: ResultRow): StreamEvent {
        // Need to load then re-serialise event to ensure format is migrated if necessary
        val rawEvent = mapper.deserialiseEvent<E>(
            row[DomainEvents.payload],
            row[DomainEvents.type],
            row[DomainEvents.version]
        )

        val serialisedPayload = mapper.serialiseEvent(rawEvent)

        return StreamEvent(
            offset = row[DomainEvents.globalOffset],
            id = EventId(row[DomainEvents.id]),
            aggregateId = AggregateId(row[DomainEvents.aggregateId]),
            aggregateType = row[DomainEvents.aggregateType],
            causationId = CausationId(row[DomainEvents.causationId]),
            correlationId = row[DomainEvents.correlationId]?.let { CorrelationId(it) },
            eventType = rawEvent::class.qualifiedName!!,
            eventTag = tag,
            payloadContentType = serialisedPayload.contentType,
            serialisedPayload = serialisedPayload.payload,
            timestamp = row[DomainEvents.timestamp],
            sequenceNumber = row[DomainEvents.sequenceNumber]
        )
    }

    data class  SaveableEvent<E : DomainEvent>(
         val id: EventId,
         val aggregateType: Aggregate<*, E, *>,
         val aggregateId: AggregateId,
         val causationId: CausationId,
         val correlationId: CorrelationId?,
         val eventType: KClass<E>,
         val rawEvent: E,
         val serialisationResult: PayloadSerialisationResult,
         val timestamp: Instant,
         val sequenceNumber: Long) {

         fun toPersistedEvent() =
             PersistedEvent(
                id = EventId(),
                aggregateId = aggregateId,
                aggregateType = aggregateType,
                causationId = causationId,
                correlationId = correlationId,
                eventType = eventType,
                eventVersion = serialisationResult.version,
                rawEvent = rawEvent,
                timestamp = Instant.now(),
                sequenceNumber = sequenceNumber
             )
    }
}