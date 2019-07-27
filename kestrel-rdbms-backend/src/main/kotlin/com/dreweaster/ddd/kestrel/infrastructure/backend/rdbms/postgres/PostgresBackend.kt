package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.postgres

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.AtomicDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.DatabaseContext
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

    private val loadEventsForTagsAfterInstantQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN ($1) AND event_timestamp > $2
            ORDER BY global_offset
            LIMIT $3
        """

    private val maxOffsetForEventsForTagsAfterInstantQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag IN ($1) AND event_timestamp > $2
        """

    private val loadEventsForTagsAfterOffsetQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN ($1) AND global_offset > $2
            ORDER BY global_offset
            LIMIT $3
        """

    private val maxOffsetForEventsForTagsAfterOffsetQueryString =
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

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId) = loadEvents(aggregateType, aggregateId, -1)

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long) = db.inTransaction { ctx ->
        ctx.select(loadEventsForAggregateInstanceQueryString, rowToPersistedEvent(aggregateType)) {
            this["$1"] = aggregateId.value
            this["$2"] = aggregateType.blueprint.name
            this["$3"] = afterSequenceNumber
        }
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId?): Flux<PersistedEvent<E>> {

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

        return db.inTransaction { ctx ->
            val saveEvents = ctx.batchUpdate(saveEventsQueryString, saveableEvents.second) { event ->
                this["event_id"] = event.id.value
                this["aggregate_id"] = aggregateId.value
                this["aggregate_type"] = aggregateType.blueprint.name
                this["tag"] = event.rawEvent.tag.value
                this["causation_id"] = event.causationId.value
                this["correlation_id"] = event.correlationId?.value
                this["event_type"] = event.eventType.qualifiedName!!
                this["event_version"] = event.serialisationResult.version
                this["event_payload"] = event.serialisationResult.payload
                this["event_timestamp"] = event.timestamp
                this["sequence_number"] = event.sequenceNumber
            }

            val saveAggregate = ctx.update(saveAggregateQueryString) {
                this["$1"] = aggregateId.value
                this["$2"] = aggregateType.blueprint.name
                this["$3"] = saveableEvents.second.last().sequenceNumber
            }

            val persistedEvents = Flux.fromIterable(saveableEvents.second.map { it.toPersistedEvent() })

            saveEvents
                .then(saveAggregate)
                .flatMap(checkForConcurrentModification)
                .thenMany(persistedEvents)
                .flatMap(updateProjections(ctx))
        }
    }

    override fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int) = db.inTransaction { ctx ->

        ctx.select(maxOffsetForEventsForTagsAfterOffsetQueryString, { it["max_offset"].longOrNull ?: -1 }) {
            this["$1"] = tags.map { tag -> tag.value }
            this["$2"] = afterOffset
        }.flatMap { maxOffset ->
            ctx.select(loadEventsForTagsAfterOffsetQueryString, rowToStreamEvent<E>()) {
                this["$1"] = tags.map { tag -> tag.value }
                this["$2"] = afterOffset
                this["$3"] = batchSize
            }.collectList().map { events ->
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
            batchSize: Int) = db.inTransaction { ctx ->

        ctx.select(maxOffsetForEventsForTagsAfterInstantQueryString, { it["max_offset"].longOrNull ?: -1 }) {
            this["$1"] = tags.map { tag -> tag.value }
            this["$2"] = afterInstant
        }.flatMap { maxOffset ->
            ctx.select(loadEventsForTagsAfterInstantQueryString, rowToStreamEvent<E>()) {
                this["$1"] = tags.map { tag -> tag.value }
                this["$2"] = afterInstant
                this["$3"] = batchSize
            }.collectList().map { events ->
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

    private fun <E: DomainEvent> rowToPersistedEvent(aggregateType: Aggregate<*,E,*>): (ResultRow) -> PersistedEvent<E>  {
        return { row ->
            val rawEvent = this.mapper.deserialiseEvent<E>(
                row["event_payload"].string,
                row["event_type"].string,
                row["event_version"].int
            )

            PersistedEvent(
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
    }

    private fun <E: DomainEvent> rowToStreamEvent(): (ResultRow) -> StreamEvent = { row ->
        // Need to load then re-serialise event to ensure format is migrated if necessary
        val rawEvent = mapper.deserialiseEvent<E>(
            row["event_payload"].string,
            row["event_type"].string,
            row["event_version"].int
        )

        val serialisedPayload = mapper.serialiseEvent(rawEvent)

        StreamEvent(
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

    data class SaveableEvent<E : DomainEvent>(
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

    private val checkForConcurrentModification = { rowsAffected: Int ->
        when (rowsAffected) {
            0 -> Mono.error(OptimisticConcurrencyException)
            else -> Mono.empty<Unit>()
        }
    }

    private fun <E: DomainEvent> updateProjections(ctx: DatabaseContext) = { event: PersistedEvent<E> ->
        Flux.concat(projections.flatMap { projection ->
            projection.update.getProjectionStatements(event).map { statement ->
                ctx.update(statement.sql, statement.parameters).flatMap { rowsAffected ->
                    when(statement.expectedRowsAffected) {
                        null -> Mono.just(event)
                        rowsAffected -> Mono.just(event)
                        else -> Mono.error(UnexpectedNumberOfRowsAffectedInUpdate(statement.expectedRowsAffected, rowsAffected))
                    }
                }
            }
        })
    }
}