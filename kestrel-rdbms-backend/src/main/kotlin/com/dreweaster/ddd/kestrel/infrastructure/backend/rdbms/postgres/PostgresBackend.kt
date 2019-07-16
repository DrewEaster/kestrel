package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.postgres

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.AtomicDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultRow
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import kotlin.reflect.KClass

class PostgresBackend(
        private val db: Database,
        private val mapper: EventPayloadMapper,
        private val projections: List<AtomicDatabaseProjection>) : Backend {

    private val loadEventsForAggregateInstanceQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE aggregate_id = $1 AND aggregate_type = $2 AND sequence_number > $3
            ORDER BY sequence_number
        """

    private val loadEventsForTagAfterInstantQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN ($1) AND event_timestamp > $2
            ORDER BY global_offset
            LIMIT $3
        """

    private val maxOffsetForEventsForTagAfterInstantQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag IN ($1) AND event_timestamp > $2
        """

    private val loadEventsForTagAfterOffsetQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN ($1) AND global_offset > $2
            ORDER BY global_offset
            LIMIT $3
        """

    private val maxOffsetForEventsForTagAfterOffsetQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag IN ($1) AND global_offset > $2
        """

    private val saveEventsQueryString =
        """
            INSERT INTO domain_event(event_id, aggregate_id, aggregate_type, tag, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """

    private val saveAggregateQueryString =
        """
            INSERT INTO aggregate_root (aggregate_id, aggregate_type, aggregate_version)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT aggregate_root_pkey
            DO UPDATE SET aggregate_version = $4 WHERE aggregate_root.aggregate_version = $5
        """

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId) = db.inTransaction { tx ->
        tx.select(
            loadEventsForAggregateInstanceQueryString,
            "$1" to aggregateId.value,
            "$2" to aggregateType.blueprint.name,
            "$3" to -1
        ) { rowToPersistedEvent(aggregateType, it) }
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long) = db.inTransaction { tx ->
        tx.select(
            loadEventsForAggregateInstanceQueryString,
            "$1" to aggregateId.value,
            "$2" to aggregateType.blueprint.name,
            "$3" to afterSequenceNumber
        ) { rowToPersistedEvent(aggregateType, it) }
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId?): Flux<PersistedEvent<E>> {

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int) = db.inTransaction { tx ->

        tx.select(maxOffsetForEventsForTagAfterOffsetQueryString,
            "$1" to tags.map { it.value },
            "$2" to afterOffset
        ) { it["max_offset"].longOrNull ?: -1 }.collectList().map { it.first() }.flatMap { maxOffset ->
            tx.select(loadEventsForTagAfterOffsetQueryString,
                "$1" to tags.map { it.value },
                "$2" to afterOffset,
                "$3" to batchSize
            ) { rowToStreamEvent<E>(it) }.collectList().map { events ->
                EventStream(
                    events = events,
                    tags = tags,
                    batchSize = batchSize,
                    startOffset = events.firstOrNull()?.offset,
                    endOffset = events.lastOrNull()?.offset,
                    maxOffset = if(maxOffset == -1L) events.lastOrNull()?.offset ?: -1L else maxOffset)
            }
        }
    }.single()


    override fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterInstant: Instant,
            batchSize: Int): Mono<EventStream> {

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun <E: DomainEvent> rowToPersistedEvent(aggregateType: Aggregate<*,E,*>, row: ResultRow): PersistedEvent<E> {
        val rawEvent = mapper.deserialiseEvent<E>(
            row["event_payload"].string,
            row["event_type"].string,
            row["event_version"].int
        )

        return PersistedEvent(
            id = EventId(row["event_id"].string),
            aggregateId = AggregateId(row["aggregate_id"].string),
            aggregateType = aggregateType,
            causationId = CausationId(row["causation_id"].string),
            correlationId = row["correlation_id"].stringOrNull?.let { CorrelationId(it) },
            eventType = rawEvent::class as KClass<E>,
            eventVersion = row["event_version"].int,
            rawEvent = rawEvent,
            timestamp = row["event_timestamp"].zonedDateTime.toInstant(),
            sequenceNumber = row["sequence_number"].long
        )
    }

    private fun <E: DomainEvent> rowToStreamEvent(row: ResultRow): StreamEvent {
        // Need to load then re-serialise event to ensure format is migrated if necessary
        val rawEvent = mapper.deserialiseEvent<E>(
            row["event_payload"].string,
            row["event_type"].string,
            row["event_version"].int
        )

        val serialisedPayload = mapper.serialiseEvent(rawEvent)

        return StreamEvent(
            offset = row["global_offset"].long,
            id = EventId(row["event_id"].string),
            aggregateId = AggregateId(row["aggregate_id"].string),
            aggregateType = row["aggregate_type"].string,
            causationId = CausationId(row["causation_id"].string),
            correlationId = row["correlation_id"].stringOrNull?.let { CorrelationId(it) },
            eventType = rawEvent::class.qualifiedName!!,
            eventTag = DomainEventTag(row["event_tag"].string),
            payloadContentType = serialisedPayload.contentType,
            serialisedPayload = serialisedPayload.payload,
            timestamp = row["event_timestamp"].instant,
            sequenceNumber = row["sequence_number"].long
        )
    }
}