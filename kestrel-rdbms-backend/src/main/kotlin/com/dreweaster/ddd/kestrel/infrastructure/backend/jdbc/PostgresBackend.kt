package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.pagination.Page
import com.dreweaster.ddd.kestrel.application.pagination.Pageable
import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CausationId
import com.dreweaster.ddd.kestrel.application.CorrelationId
import com.dreweaster.ddd.kestrel.application.EventId
import com.dreweaster.ddd.kestrel.application.ProcessManagerCorrelationId
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.ProcessManager
import io.vavr.control.Try
import org.jetbrains.exposed.sql.*
import java.time.Instant
import kotlin.reflect.KClass

class PostgresBackend(
        private val db: Database,
        private val mapper: EventPayloadMapper,
        private val readModels: List<SynchronousJdbcReadModel>) : Backend {

    object DomainEvents : Table("domain_event") {
        val globalOffset  = long("global_offset").primaryKey().autoIncrement()
        val id = varchar("event_id", 144)
        val aggregateId = varchar("aggregate_id", 144)
        val aggregateType = varchar("aggregate_type", 144)
        val tag = varchar("tag", 100)
        val causationId = varchar("causation_id", 144)
        val correlationId = varchar("correlation_id", 144).nullable()
        val type = varchar("event_type", 255)
        val version = integer("event_version")
        val payload = text("event_payload")
        val timestamp = instant("event_timestamp")
        val sequenceNumber = long("sequence_number")
    }

    object AggregateRoots : Table("aggregate_root") {
        val id = varchar("aggregate_id", 144).primaryKey(0)
        val type = varchar("aggregate_type", 255).primaryKey(1)
        val version = long("aggregate_version")
        val primaryKeyConstraintConflictTarget = primaryKeyConstraintConflictTarget(id, type)
    }

    object ProcessManagerDomainEvents: Table("process_manager_domain_events") {
        val globalOffset  = long("global_offset").primaryKey().autoIncrement()
        val id = varchar("event_id", 144)
        val processManagerCorrelationId = varchar("process_manager_correlation_id", 144).uniqueIndex("event_id_unique_index")
        val processManagerType = varchar("process_manager_type", 144)
        val tag = varchar("tag", 100)
        val type = varchar("event_type", 255)
        val version = integer("event_version")
        val payload = text("event_payload")
        val timestamp = instant("event_timestamp")
        val sequenceNumber = long("sequence_number")
        val d = index(true, id, processManagerCorrelationId)
        val uniqueEventForProcessManagerInstanceConflictTarget = uniqueIndexConflictTarget("event_id_unique_index", id, processManagerCorrelationId)

        val primaryKey = primaryKey(globalOffset)
        val uniqueEventIndex = uniqueIndex("event_id_unique_index", id, processManagerCorrelationId)
    }

    object ProcessManagerDomainEvent: Table("process_manager_domain_events") {
        val globalOffset  = long("global_offset")
        val id = string("event_id")
        val processManagerCorrelationId = string("process_manager_correlation_id")
        val processManagerType = string("process_manager_type", 144)
        val tag = string("tag", 100)
        val type = string("event_type", 255)
        val version = int("event_version")
        val payload = string("event_payload")
        val timestamp = instant("event_timestamp")
        val sequenceNumber = long("sequence_number")
    }

    object ProcessManagers: Table("process_manager") {
        val id = varchar("process_manager_correlation_id", 144)
        val type = varchar("process_manager_type", 144)
        val primaryKeyConstraintConflictTarget = primaryKeyConstraintConflictTarget(id, type)
        val minSequenceNumber = long("min_sequence_number")
        val maxSequenceNumber = long("max_sequence_number")
        val lastProcessedSequenceNumber = long("last_processed_sequence_number")
        val oldestUnprocessedTimestamp = instant("oldest_unprocessed_timestamp")
        val hasUnprocessedEvents = bool("has_unprocessed_events")
        val retryCount = integer("retry_count")
        val retryAfter = integer("retry_after").nullable()
        val suspended = bool("suspended")
        val processingLastAttemptedAt = instant("processing_last_attempted_at").nullable()
    }

    object ProcessManagerFailures: Table("process_manager_failure") {
        val id = long("failure_id").primaryKey().autoIncrement()
        val processManagerCorrelationId = varchar("process_manager_correlation_id", 144)
        val sequenceNumber = long("sequence_number")
        val failureCode = varchar("failure_code", 36)
        val stackTrace = text("stack_trace").nullable()
        val message = text("message").nullable()
        val failureTimestamp = instant("failure_timestamp")
    }

    override suspend fun <E : DomainEvent, P : ProcessManager<*, E, *>> persistProcessManagerEvent(
            eventId: EventId,
            rawEvent: E,
            processManagerType: P,
            processManagerCorrelationId: ProcessManagerCorrelationId,
            causationId: CausationId) {

        val expectedMaxSequenceNumber = db.transaction {
            ProcessManagers.slice(ProcessManagers.id).select {
                ProcessManagers.id eq processManagerCorrelationId.value
            }.firstOrNull()?.let { it[ProcessManagers.maxSequenceNumber] }
        } ?: -1

        val serialisationResult = mapper.serialiseEvent(rawEvent)
        val nextSequenceNumber = expectedMaxSequenceNumber + 1

        db.transaction { _ ->

            val insertEventRowsAffected = ProcessManagerDomainEvents.insertOnConflictDoNothing(ProcessManagerDomainEvents.uniqueEventForProcessManagerInstanceConflictTarget) {
                it[ProcessManagerDomainEvents.id] = eventId.value
                it[ProcessManagerDomainEvents.processManagerCorrelationId] = processManagerCorrelationId.value
                it[ProcessManagerDomainEvents.processManagerType] = processManagerType.blueprint.name
                it[ProcessManagerDomainEvents.tag] = rawEvent.tag.value
                it[ProcessManagerDomainEvents.type] = rawEvent::class.qualifiedName!!
                it[ProcessManagerDomainEvents.version] = serialisationResult.version
                it[ProcessManagerDomainEvents.payload] = serialisationResult.payload
                it[ProcessManagerDomainEvents.timestamp] = Instant.now()
                it[ProcessManagerDomainEvents.sequenceNumber] = nextSequenceNumber
            }

            // If the event id already exists for the given process manager id (i.e. no rows inserted) then we skip updating the process manager state
            if(insertEventRowsAffected != 0) {
                val setProcessManagerStateRowsAffected = ProcessManagers.upsert(ProcessManagers.primaryKeyConstraintConflictTarget, { ProcessManagers.maxSequenceNumber eq expectedMaxSequenceNumber }) {
                    it[id] = processManagerCorrelationId.value
                    it[type] = processManagerType.blueprint.name
                    it[maxSequenceNumber] = nextSequenceNumber
                }

                if (setProcessManagerStateRowsAffected == 0) throw OptimisticConcurrencyException
            }
        }
    }

    override suspend fun findIdsForProcessManagersAwaitingProcessing(pageable: Pageable): Page<ProcessManagerCorrelationId> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }



    // TODO: Will need to implement snapshot retrieval
    // TODO: Should this try to loop through executing all outstanding unprocessed events, or just the next one?
    // If it loops, each iteration should (would have to) be in a separate transaction
    override suspend fun <E : DomainEvent, P : ProcessManager<*, E, *>> executeProcessManager(
            type: P,
            id: ProcessManagerCorrelationId,
            force: Boolean,
            retryStrategy: ProcessManagerRetryStrategy,
            processHandler: suspend (PersistedProcessManager<E, P>) -> ProcessManagerProcessingResult): ProcessManagerProcessingResult {

        data class ProcessManagerMetadata(
                val minSequenceNumber: Long,
                val lastProcessedSequenceNumber: Long,
                val nextOldestUnprocessedTimestamp: Instant?,
                val retryCount: Int)

        data class PersistedProcessManagerEvent<E: DomainEvent>(
                val eventId: EventId,
                val rawEvent: E,
                val timestamp: Instant,
                val sequenceNumber: Long)

        val processManager = db.transaction { _ ->
            // TODO: properly handle case where process manager is not found with id and type
           ProcessManagers.select {
                (ProcessManagers.id eq id.value) and
                (ProcessManagers.type eq type.blueprint.name) and
                (ProcessManagers.hasUnprocessedEvents eq true) and
                (ProcessManagers.suspended eq false).
            }.firstOrNull()?.let { row -> Triple(
                row[ProcessManagers.minSequenceNumber],
                row[ProcessManagers.lastProcessedSequenceNumber],
                row[ProcessManagers.retryCount]
            )}?.let {
                val (minSequenceNumber, lastProcessedSequenceNumber, retryCount) = it
                val events = ProcessManagerDomainEvents.select {
                    (ProcessManagerDomainEvents.processManagerCorrelationId eq id.value) and
                            (ProcessManagerDomainEvents.type eq type.blueprint.name) and
                            (ProcessManagerDomainEvents.sequenceNumber lessEq (lastProcessedSequenceNumber + 2)) and
                            (ProcessManagerDomainEvents.sequenceNumber greaterEq minSequenceNumber)
                }.orderBy(ProcessManagerDomainEvents.sequenceNumber).map { row ->
                    val rawEvent = mapper.deserialiseEvent<E>(
                            row[DomainEvents.payload],
                            row[DomainEvents.type],
                            row[DomainEvents.version]
                    )
                    PersistedProcessManagerEvent(
                        eventId = EventId(row[ProcessManagerDomainEvents.id]),
                        rawEvent = rawEvent,
                        timestamp = row[ProcessManagerDomainEvents.timestamp],
                        sequenceNumber = row[ProcessManagerDomainEvents.sequenceNumber]
                    )
                }

               ProcessManagers
                   .selectAll()
                   .apply { if (events.isNotEmpty()) andWhere { ProcessManagers.minSequenceNumber eq 0L } }
                   .apply { if (events.isEmpty()) andWhere { ProcessManagers.minSequenceNumber greater 1L } }

                PersistedProcessManager(
                    processManagerCorrelationId = id,
                    processManagerType = type,
                    processedEvents = events.dropLastWhile { e -> e.sequenceNumber >= lastProcessedSequenceNumber + 2 }.map { e -> Pair(e.eventId, e.rawEvent) },
                    nextEventToProcess = events.findLast { e -> e.sequenceNumber == lastProcessedSequenceNumber + 1 }?.let { e -> Pair(e.eventId, e.rawEvent) }
                ) to ProcessManagerMetadata(
                    minSequenceNumber = minSequenceNumber,
                    lastProcessedSequenceNumber = lastProcessedSequenceNumber,
                    nextOldestUnprocessedTimestamp = events.findLast { e -> e.sequenceNumber == lastProcessedSequenceNumber + 2 }?.timestamp,
                    retryCount = retryCount
                )
            }
        }

        return if(processManager != null) {
            if(processManager.first.nextEventToProcess != null) {
                try {
                    val result = processHandler(processManager.first)
                    when(result) {
                        is Continue -> {
                            val rowsAffected = ProcessManagers.upsert(ProcessManagers.primaryKeyConstraintConflictTarget(), {
                                ProcessManagers.lastProcessedSequenceNumber eq processManager.second.lastProcessedSequenceNumber
                            }) {
                                it[ProcessManagers.id] = processManager.first.processManagerCorrelationId.value
                                it[ProcessManagers.type] = processManager.first.processManagerType.blueprint.name
                                it[ProcessManagers.lastProcessedSequenceNumber] = processManager.second.lastProcessedSequenceNumber + 1
                                it[ProcessManagers.suspended] = false
                                it[ProcessManagers.oldestUnprocessedTimestamp] = processManager.second.nextOldestUnprocessedTimestamp ?: Instant.now()
                                it[ProcessManagers.hasUnprocessedEvents] = processManager.second.nextOldestUnprocessedTimestamp != null
                                it[ProcessManagers.retryCount] = 0
                                it[ProcessManagers.retryAfter] = null
                                it[ProcessManagers.processingLastAttemptedAt] = Instant.now()
                            }
                            if(rowsAffected > 0) result else AlreadyProcessed
                        }
                        is Finished -> {
                            val rowsAffected = ProcessManagers.upsert(ProcessManagers.primaryKeyConstraintConflictTarget(), {
                                ProcessManagers.lastProcessedSequenceNumber eq processManager.second.lastProcessedSequenceNumber
                            }) {
                                it[ProcessManagers.id] = processManager.first.processManagerCorrelationId.value
                                it[ProcessManagers.type] = processManager.first.processManagerType.blueprint.name
                                it[ProcessManagers.lastProcessedSequenceNumber] = processManager.second.lastProcessedSequenceNumber + 1
                                it[ProcessManagers.minSequenceNumber] = processManager.second.lastProcessedSequenceNumber + 2
                                it[ProcessManagers.suspended] = false
                                it[ProcessManagers.oldestUnprocessedTimestamp] = processManager.second.nextOldestUnprocessedTimestamp ?: Instant.now()
                                it[ProcessManagers.hasUnprocessedEvents] = processManager.second.nextOldestUnprocessedTimestamp != null
                                it[ProcessManagers.retryCount] = 0
                                it[ProcessManagers.retryAfter] = null
                                it[ProcessManagers.processingLastAttemptedAt] = Instant.now()
                            }
                            if(rowsAffected > 0) result else AlreadyProcessed
                        }
                        is Failed -> {
                            // TODO: Update last processed sequence number - ignore optimistic concurrency error
                            // TODO: Save failure info to database
                            // TODO: set retry count and retry after accordingly
                            // TODO: Set has_unprocessed_events flag accordingly
                            // TODO: Set suspended accordingly
                            // TODO: Set last_process_attempted_at
                            result
                        }
                        is NothingToProcess -> result
                        is AlreadyProcessed -> result
                    }
                } catch (ex: Throwable) {
                    Failed(failureCode = "unhandled_exception", ex = ex)
                }
            } else NothingToProcess
        } else NothingToProcess
    }

    override suspend fun <E : DomainEvent> loadEventStream(tags: Set<DomainEventTag>, afterOffset: Long, batchSize: Int): EventStream {
        return db.transaction {
            val maxExpr = DomainEvents.globalOffset.max()
            val maxOffset = DomainEvents.slice(maxExpr).select {
                (DomainEvents.tag inList tags.map { it.value }) and
                (DomainEvents.globalOffset greater afterOffset)
            }.firstOrNull()?.let { row -> row[maxExpr] } ?: -1L

            val events = DomainEvents.select {
                (DomainEvents.tag inList tags.map { it.value }) and
                (DomainEvents.globalOffset greater afterOffset)
            }.orderBy(DomainEvents.globalOffset).limit(batchSize).map { row ->
                rowToStreamEvent<E>(row)
            }

            EventStream(
                events = events,
                tags = tags,
                batchSize = batchSize,
                startOffset = events.firstOrNull()?.offset,
                endOffset = events.lastOrNull()?.offset,
                maxOffset = if(maxOffset == -1L) events.lastOrNull()?.offset ?: -1L else maxOffset)
        }
    }

    override suspend fun <E : DomainEvent> loadEventStream(tags: Set<DomainEventTag>, afterInstant: Instant, batchSize: Int): EventStream {
        return db.transaction {
            val maxExpr = DomainEvents.globalOffset.max()
            val maxOffset = DomainEvents.slice(maxExpr).select {
                (DomainEvents.tag inList tags.map { it.value }) and
                (DomainEvents.timestamp greater afterInstant)
            }.firstOrNull()?.let { row -> row[maxExpr] } ?: -1L

            val events = DomainEvents.select {
                (DomainEvents.tag inList tags.map { it.value }) and
                (DomainEvents.timestamp greater afterInstant)
            }.orderBy(DomainEvents.globalOffset).limit(batchSize).map { row ->
                rowToStreamEvent<E>(row)
            }

            EventStream(
                    events = events,
                    tags = tags,
                    batchSize = batchSize,
                    startOffset = events.firstOrNull()?.offset,
                    endOffset = events.lastOrNull()?.offset,
                    maxOffset = if(maxOffset == -1L) events.lastOrNull()?.offset ?: -1L else maxOffset
            )
        }
    }

    override suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(aggregateType: A, aggregateId: AggregateId): List<PersistedEvent<E>> {
        return db.transaction {
            DomainEvents.select {
                (DomainEvents.aggregateId eq aggregateId.value) and
                (DomainEvents.aggregateType eq aggregateType.blueprint.name) and
                (DomainEvents.sequenceNumber greater -1L)
            }.orderBy(DomainEvents.sequenceNumber).map { row -> rowToPersistedEvent(aggregateType, row) }
        }
    }

    override suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long): List<PersistedEvent<E>> {
        return db.transaction {
            DomainEvents.select {
                (DomainEvents.aggregateId eq aggregateId.value) and
                (DomainEvents.aggregateType eq aggregateType.blueprint.name) and
                (DomainEvents.sequenceNumber greater afterSequenceNumber)
            }.orderBy(DomainEvents.sequenceNumber).map { row -> rowToPersistedEvent(aggregateType, row) }
        }
    }

    override suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> saveEvents(aggregateType: A, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): List<PersistedEvent<E>> {
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

        db.

        db.transaction { _ ->

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
            }.

            if(saveAggregateRowsAffected == 0) throw OptimisticConcurrencyException

            // Update synchronous read models
            readModels.forEach {
                val projection = it.update
                if(projection.aggregateType == aggregateType::class) {
                    persistedEvents.forEach { e ->
                        projection.handleE vent(e as PersistedEvent<DomainEvent>)
                    }
                }
            }
        }

        return persistedEvents
    }

    override suspend fun <E : DomainEvent, A : Aggregate<*, E, *>> persistAggregate(
            aggregateType: A,
            aggregateId: AggregateId,
            commandHandler: suspend (PersistedAggregate<E, A>) -> GeneratedEvents<E>): Try<List<PersistedEvent<E>>> {

        val previousEvents = loadEvents(aggregateType, aggregateId)
        val expectedSequenceNumber = previousEvents.lastOrNull()?.sequenceNumber ?: -1

        val persistedEvents = try {
            val result = commandHandler(PersistedAggregate(
                aggregateType = aggregateType,
                aggregateId = aggregateId,
                previousEvents = previousEvents.map { it.rawEvent })
            )
            saveEvents(
                aggregateType = aggregateType,
                aggregateId = aggregateId,
                causationId = result.causationId,
                rawEvents = result.events,
                correlationId = result.correlationId,
                expectedSequenceNumber = expectedSequenceNumber
            )
        } catch (ex: Throwable) {
            return Try.failure<List<PersistedEvent<E>>>(ex)
        }
        return Try.success(persistedEvents)
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

    private fun <E: DomainEvent> rowToStreamEvent(row: ResultRow): StreamEvent {
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
            eventTag = DomainEventTag(row[DomainEvents.tag]),
            payloadContentType = serialisedPayload.contentType,
            serialisedPayload = serialisedPayload.payload,
            timestamp = row[DomainEvents.timestamp],
            sequenceNumber = row[DomainEvents.sequenceNumber]
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
}