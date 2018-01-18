package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.github.andrewoma.kwery.core.Row
import java.sql.Timestamp
import java.time.Instant
import kotlin.reflect.KClass

class PostgresBackend(
        private val db: Database,
        private val mapper: EventPayloadMapper,
        private val readModels: List<SynchronousJdbcReadModel>) : Backend {

    private val loadEventsForAggregateInstanceQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type AND sequence_number > :sequence_number
            ORDER BY sequence_number
        """

    private val loadEventsForTagAfterInstantQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag = :tag AND event_timestamp > :after_instant
            ORDER BY global_offset
            LIMIT :limit
        """

    private val maxOffsetForEventsForTagAfterInstantQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag = :tag AND event_timestamp > :after_instant
        """

    private val loadEventsForTagAfterOffsetQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag = :tag AND global_offset > :after_offset
            ORDER BY global_offset
            LIMIT :limit
        """

    private val maxOffsetForEventsForTagAfterOffsetQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag = :tag AND global_offset > :after_offset
        """

    private val saveEventsQueryString =
        """
            INSERT INTO domain_event(event_id, aggregate_id, aggregate_type, tag, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number)
            VALUES(:event_id,:aggregate_id,:aggregate_type,:tag,:causation_id,:correlation_id,:event_type,:event_version,:event_payload,:event_timestamp,:sequence_number)
        """

    private val saveAggregateQueryString =
        """
            INSERT INTO aggregate_root (aggregate_id,aggregate_type,aggregate_version)
            VALUES (:aggregate_id,:aggregate_type,:new_aggregate_version)
            ON CONFLICT ON CONSTRAINT aggregate_root_pkey
            DO UPDATE SET aggregate_version = :new_aggregate_version WHERE aggregate_root.aggregate_version = :expected_previous_aggregate_version
        """

    @Suppress("UNCHECKED_CAST")
    suspend override fun <E : DomainEvent> loadEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId): List<PersistedEvent<E>> {
        return db.withSession { session ->
            session.select(loadEventsForAggregateInstanceQueryString, mapOf(
                    "aggregate_id" to aggregateId.value,
                    "aggregate_type" to aggregateType.blueprint.name,
                    "sequence_number" to -1
            )) { rowToPersistedEvent(aggregateType, it) }
        }
    }

    suspend override fun <E : DomainEvent> loadEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId, afterSequenceNumber: Long): List<PersistedEvent<E>> {
        return db.withSession { session ->
            session.select(loadEventsForAggregateInstanceQueryString, mapOf(
                    "aggregate_id" to aggregateId.value,
                    "aggregate_type" to aggregateType.blueprint.name,
                    "sequence_number" to afterSequenceNumber
            )) { rowToPersistedEvent(aggregateType, it) }
        }
    }

    suspend override fun <E : DomainEvent> loadEventStream(tag: DomainEventTag, afterOffset: Long, batchSize: Int): EventStream {
        val maxOffset = db.withSession { session ->
            session.select(maxOffsetForEventsForTagAfterOffsetQueryString, mapOf(
                    "tag" to tag.value,
                    "after_offset" to afterOffset
            )) { it.long("max_offset") }.firstOrNull() ?: -1
        }
        val events = db.withSession { session ->
            session.select(loadEventsForTagAfterOffsetQueryString, mapOf(
                    "tag" to tag.value,
                    "after_offset" to afterOffset,
                    "limit" to batchSize
            )) { rowToStreamEvent<E>(tag, it) }
        }
        return EventStream(events, tag, batchSize, events.firstOrNull()?.let { it.sequenceNumber }, events.lastOrNull()?.let { it.sequenceNumber }, maxOffset)
    }

    suspend override fun <E : DomainEvent> loadEventStream(tag: DomainEventTag, afterInstant: Instant, batchSize: Int): EventStream {
        val maxOffset = db.withSession { session ->
            session.select(maxOffsetForEventsForTagAfterInstantQueryString, mapOf(
                    "tag" to tag.value,
                    "after_instant" to Timestamp.from(afterInstant)
            )) { it.long("max_offset") }.firstOrNull() ?: -1
        }
        val events =  db.withSession { session ->
            session.select(loadEventsForTagAfterInstantQueryString, mapOf(
                    "tag" to tag.value,
                    "after_instant" to Timestamp.from(afterInstant),
                    "limit" to batchSize
            )) { rowToStreamEvent<E>(tag, it) }
        }
        return EventStream(events, tag, batchSize, events.firstOrNull()?.let { it.sequenceNumber }, events.lastOrNull()?.let { it.sequenceNumber }, maxOffset)
    }

    suspend override fun <E : DomainEvent> saveEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): List<PersistedEvent<E>> {
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
            db.withSession { session ->
                // Save events
                val saveEventRowsAffected = session.batchUpdate(saveEventsQueryString, saveableEvents.second.map { it.toMap() })
                if(saveEventRowsAffected.isEmpty()) tx.rollbackOnly = true

                // Save aggregate
                val saveAggregateRowsAffected = session.update(saveAggregateQueryString, mapOf(
                        "aggregate_id" to aggregateId.value,
                        "aggregate_type" to aggregateType.blueprint.name,
                        "new_aggregate_version" to saveableEvents.second.last().sequenceNumber,
                        "expected_previous_aggregate_version" to expectedSequenceNumber
                ))

                if(saveAggregateRowsAffected == 0) throw OptimisticConcurrencyException

                // Update synchronous read models
                readModels.forEach {
                    if(it.aggregateType() == aggregateType) {
                        persistedEvents.forEach { e ->
                            it.update(e, session, tx)
                        }
                    }
                }
            }
        }

        return persistedEvents
    }

    private fun <E: DomainEvent> rowToPersistedEvent(aggregateType: Aggregate<*,E,*>, row: Row): PersistedEvent<E> {
        val rawEvent = mapper.deserialiseEvent<E>(
                row.string("event_payload"),
                row.string("event_type"),
                row.int("event_version")
        )

        return PersistedEvent(
                id = EventId(row.string("event_id")),
                aggregateId = AggregateId(row.string("aggregate_id")),
                aggregateType = aggregateType,
                causationId = CausationId(row.string("causation_id")),
                correlationId = row.stringOrNull("correlation_id")?.let { CorrelationId(it) },
                eventType = rawEvent::class as KClass<E>,
                eventVersion = row.int("event_version"),
                rawEvent = rawEvent,
                timestamp = row.timestamp("event_timestamp").toInstant(),
                sequenceNumber = row.long("sequence_number")
        )
    }

    private fun <E: DomainEvent> rowToStreamEvent(tag: DomainEventTag, row: Row): StreamEvent {
        // Need to load then re-serialise event to ensure format is migrated if necessary
        val rawEvent = mapper.deserialiseEvent<E>(
                row.string("event_payload"),
                row.string("event_type"),
                row.int("event_version")
        )

        val serialisedPayload = mapper.serialiseEvent(rawEvent)

        return StreamEvent(
                offset = row.long("global_offset"),
                id = EventId(row.string("event_id")),
                aggregateId = AggregateId(row.string("aggregate_id")),
                aggregateType = row.string("aggregate_type"),
                causationId = CausationId(row.string("causation_id")),
                correlationId = row.stringOrNull("correlation_id")?.let { CorrelationId(it) },
                eventType = rawEvent::class.qualifiedName!!,
                eventTag = tag,
                payloadContentType = serialisedPayload.contentType,
                serialisedPayload = serialisedPayload.payload,
                timestamp = row.timestamp("event_timestamp").toInstant(),
                sequenceNumber = row.long("sequence_number")
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

         fun toMap() = mapOf(
                 "event_id" to id.value,
                 "aggregate_id" to aggregateId.value,
                 "aggregate_type" to aggregateType.blueprint.name,
                 "tag" to rawEvent.tag.value,
                 "causation_id" to causationId.value,
                 "correlation_id" to correlationId?.value,
                 "event_type" to eventType.qualifiedName!!,
                 "event_version" to serialisationResult.version,
                 "event_payload" to serialisationResult.payload,
                 "event_timestamp" to Timestamp.from(timestamp),
                 "sequence_number" to sequenceNumber
         )

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