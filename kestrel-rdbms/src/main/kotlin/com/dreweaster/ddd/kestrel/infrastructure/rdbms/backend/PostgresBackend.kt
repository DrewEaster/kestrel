package com.dreweaster.ddd.kestrel.infrastructure.rdbms.backend

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.ConsistentDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.DatabaseContext
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.ResultRow
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import kotlin.reflect.KClass

class PostgresBackend(
        private val db: Database,
        private val mapper: EventPayloadMapper,
        private val projections: List<ConsistentDatabaseProjection>) : Backend {

    private val maxOffsetForAllEventsQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
        """

    private val loadEventsForAggregateInstanceQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE aggregate_id = :aggregate_id AND aggregate_type = :aggregate_type AND sequence_number > :sequence_number
            ORDER BY sequence_number
        """

    private val loadEventsForTagsAfterInstantQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, tag, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN (:tags) AND event_timestamp > :after_timestamp
            ORDER BY global_offset
            LIMIT :limit
        """

    private val maxQueryOffsetForEventsForTagsAfterInstantQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag IN (:tags) AND event_timestamp > :after_timestamp
        """

    private val loadEventsForTagsAfterOffsetQueryString =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, tag, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN (:tags) AND global_offset > :after_offset
            ORDER BY global_offset
            LIMIT :limit
        """

    private val maxQueryOffsetForEventsForTagsAfterOffsetQueryString =
        """
            SELECT MAX(global_offset) as max_offset
            FROM domain_event
            WHERE tag IN (:tags) AND global_offset > :after_offset
        """

    private val saveEventsQueryString =
        """
            INSERT INTO domain_event(event_id, aggregate_id, aggregate_type, tag, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number)
            VALUES(:event_id, :aggregate_id, :aggregate_type, :tag, :causation_id, :correlation_id, :event_type, :event_version, :event_payload, :event_timestamp, :sequence_number)
        """

    private val saveAggregateQueryString =
        """
            INSERT INTO aggregate_root (aggregate_id, aggregate_type, aggregate_version)
            VALUES (:aggregate_id, :aggregate_type, :aggregate_version)
            ON CONFLICT ON CONSTRAINT aggregate_root_pkey
            DO UPDATE SET aggregate_version = :aggregate_version WHERE aggregate_root.aggregate_version = :expected_aggregate_version
        """

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId) = loadEvents(aggregateType, aggregateId, -1)

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long) = db.inTransaction { ctx ->
        ctx.select(loadEventsForAggregateInstanceQueryString, rowToPersistedEvent(aggregateType)) {
            this["aggregate_id"] = aggregateId.value
            this["aggregate_type"] = aggregateType.blueprint.name
            this["sequence_number"] = afterSequenceNumber
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

        val doSaveEvents: (DatabaseContext) -> Flux<PersistedEvent<E>> = { ctx ->

            val saveEvents = ctx.batchUpdate(saveEventsQueryString, saveableEvents.second) { event ->
                this["event_id"] = event.id.value
                this["aggregate_id"] = aggregateId.value
                this["aggregate_type"] = aggregateType.blueprint.name
                this["tag"] = event.rawEvent.tag.value
                this["causation_id"] = event.causationId.value
                this["correlation_id"] = nullable(event.correlationId?.value)
                this["event_type"] = event.eventType.qualifiedName!!
                this["event_version"] = event.serialisationResult.version
                this["event_payload"] = event.serialisationResult.payload
                this["event_timestamp"] = event.timestamp
                this["sequence_number"] = event.sequenceNumber
            }

            val saveAggregate = ctx.update(saveAggregateQueryString) {
                this["aggregate_id"] = aggregateId.value
                this["aggregate_type"] = aggregateType.blueprint.name
                this["aggregate_version"] = saveableEvents.second.last().sequenceNumber
                this["expected_aggregate_version"] = expectedSequenceNumber
            }

            val persistedEvents = Flux.fromIterable(saveableEvents.second.map { it.toPersistedEvent() })

            saveEvents
                .thenMany(saveAggregate)
                .flatMap(checkForConcurrentModification)
                .thenMany(persistedEvents)
                .flatMap(updateProjections(ctx))
        }

        return db.inTransaction(doSaveEvents)
    }

    override fun <E : DomainEvent> fetchEventFeed(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int): Mono<EventFeed> = db.inTransaction { ctx ->

        ctx.select(maxOffsetForAllEventsQueryString) { it["max_offset"].longOrNull ?: -1  }.flatMap { globalMaxOffset ->
            ctx.select(maxQueryOffsetForEventsForTagsAfterOffsetQueryString, { it["max_offset"].longOrNull ?: -1 }) {
                this["tags"] = tags.map { tag -> tag.value }
                this["after_offset"] = afterOffset
            }.flatMap { queryMaxOffset ->
                ctx.select(loadEventsForTagsAfterOffsetQueryString, rowToStreamEvent<E>()) {
                    this["tags"] = tags.map { tag -> tag.value }
                    this["after_offset"] = afterOffset
                    this["limit"] = batchSize
                }.collectList().map { events ->
                    val derivedQueryMaxOffset = if (queryMaxOffset == -1L) events.lastOrNull()?.offset ?: -1L else queryMaxOffset
                    val derivedGlobalMaxOffset = if(globalMaxOffset == -1L ) derivedQueryMaxOffset else maxOf(globalMaxOffset, derivedQueryMaxOffset)
                    EventFeed(
                        events = events,
                        tags = tags,
                        pageSize = batchSize,
                        pageStartOffset = events.firstOrNull()?.offset,
                        pageEndOffset = events.lastOrNull()?.offset,
                        queryMaxOffset = derivedQueryMaxOffset,
                        globalMaxOffset = derivedGlobalMaxOffset
                    )
                }
            }
        }
    }.single()

    // TODO: Remove duplication
    override fun <E : DomainEvent> fetchEventFeed(
            tags: Set<DomainEventTag>,
            afterInstant: Instant,
            batchSize: Int): Mono<EventFeed> = db.inTransaction { ctx ->

        ctx.select(maxOffsetForAllEventsQueryString) { it["max_offset"].longOrNull ?: -1  }.flatMap { globalMaxOffset ->
            ctx.select(maxQueryOffsetForEventsForTagsAfterInstantQueryString, { it["max_offset"].longOrNull ?: -1 }) {
                this["tags"] = tags.map { tag -> tag.value }
                this["after_timestamp"] = afterInstant
            }.flatMap { queryMaxOffset ->
                ctx.select(loadEventsForTagsAfterInstantQueryString, rowToStreamEvent<E>()) {
                    this["tags"] = tags.map { tag -> tag.value }
                    this["after_timestamp"] = afterInstant
                    this["limit"] = batchSize
                }.collectList().map { events ->
                    val derivedQueryMaxOffset = if (queryMaxOffset == -1L) events.lastOrNull()?.offset ?: -1L else queryMaxOffset
                    val derivedGlobalMaxOffset = if(globalMaxOffset == -1L ) derivedQueryMaxOffset else maxOf(globalMaxOffset, derivedQueryMaxOffset)
                    EventFeed(
                        events = events,
                        tags = tags,
                        pageSize = batchSize,
                        pageStartOffset = events.firstOrNull()?.offset,
                        pageEndOffset = events.lastOrNull()?.offset,
                        queryMaxOffset = derivedQueryMaxOffset,
                        globalMaxOffset = derivedGlobalMaxOffset
                    )
                }
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

    private fun <E: DomainEvent> rowToStreamEvent(): (ResultRow) -> FeedEvent = { row ->
        // Need to load then re-serialise event to ensure format is migrated if necessary
        val rawEvent = mapper.deserialiseEvent<E>(
            row["event_payload"].string,
            row["event_type"].string,
            row["event_version"].int
        )

        val serialisedPayload = mapper.serialiseEvent(rawEvent)

        FeedEvent(
            offset = row["global_offset"].long,
            id = EventId(row["event_id"].string),
            aggregateId = AggregateId(row["aggregate_id"].string),
            aggregateType = row["aggregate_type"].string,
            causationId = CausationId(row["causation_id"].string),
            correlationId = row["correlation_id"].stringOrNull?.let { CorrelationId(it) },
            eventType = rawEvent::class.qualifiedName!!,
            eventTag = DomainEventTag(row["tag"].string),
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

    private val checkForConcurrentModification: (Int) -> Mono<Void> = { rowsAffected ->
        when (rowsAffected) {
            0 -> Mono.error(OptimisticConcurrencyException)
            else -> Mono.empty<Void>()
        }
    }

    private fun <E: DomainEvent> updateProjections(ctx: DatabaseContext): (PersistedEvent<E>) -> Flux<PersistedEvent<E>> = { event ->
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