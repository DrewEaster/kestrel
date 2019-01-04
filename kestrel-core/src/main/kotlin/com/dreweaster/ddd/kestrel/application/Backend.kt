package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.application.pagination.Page
import com.dreweaster.ddd.kestrel.application.pagination.Pageable
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.ProcessManager
import io.vavr.control.Try
import java.time.Instant
import kotlin.reflect.KClass

interface Backend {

    suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> persistAggregate(
            aggregateType: A,
            aggregateId: AggregateId,
            commandHandler: suspend (PersistedAggregate<E, A>) -> GeneratedEvents<E>): Try<List<PersistedEvent<E>>>

    suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId): List<PersistedEvent<E>>

    suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            afterSequenceNumber: Long): List<PersistedEvent<E>>

    suspend fun <E : DomainEvent, A: Aggregate<*,E,*>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId? = null): List<PersistedEvent<E>>

    suspend fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int): EventStream

    suspend fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterInstant: Instant,
            batchSize: Int): EventStream

    suspend fun <E: DomainEvent, P: ProcessManager<*,E,*>> persistProcessManagerEvent(
            eventId: EventId,
            rawEvent: E,
            processManagerType: P,
            processManagerCorrelationId: ProcessManagerCorrelationId,
            causationId: CausationId)

    suspend fun findIdsForProcessManagersAwaitingProcessing(pageable: Pageable): Page<ProcessManagerCorrelationId>

    suspend fun <E: DomainEvent, P: ProcessManager<*,E,*>> executeProcessManager(
            type: P,
            id: ProcessManagerCorrelationId,
            force: Boolean = false,
            retryStrategy: ProcessManagerRetryStrategy,
            processHandler: suspend (PersistedProcessManager<E,P>) -> ProcessManagerProcessingResult): ProcessManagerProcessingResult
}

interface ProcessManagerRetryStrategy { fun retryAfter(totalRetriesAttempted: Int): Instant? }

sealed class ProcessManagerProcessingResult
object Continue: ProcessManagerProcessingResult()
object Finished: ProcessManagerProcessingResult()
object NothingToProcess: ProcessManagerProcessingResult()
object AlreadyProcessed: ProcessManagerProcessingResult()
data class Failed(val failureCode: String, val message: String? = null, val ex: Throwable?): ProcessManagerProcessingResult()

data class GeneratedEvents<E: DomainEvent>(
        val events: List<E>,
        val causationId: CausationId,
        val correlationId: CorrelationId? = null)

object OptimisticConcurrencyException : RuntimeException()

data class PersistedProcessManager<E: DomainEvent, P: ProcessManager<*,E,*>>(
        val processManagerType: P,
        val processManagerCorrelationId: ProcessManagerCorrelationId,
        val processedEvents: List<Pair<EventId, E>>,
        val nextEventToProcess: Pair<EventId, E>?)

data class PersistedAggregate<E: DomainEvent, A: Aggregate<*,E,*>>(
        val aggregateType: A,
        val aggregateId: AggregateId,
        val previousEvents: List<E>)

data class PersistedEvent<E : DomainEvent>(
        val id: EventId,
        val aggregateType: Aggregate<*, E, *>,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val eventType: KClass<E>,
        val eventVersion: Int,
        val rawEvent: E,
        val timestamp: Instant,
        val sequenceNumber: Long)

data class EventStream(
        val events: List<StreamEvent>,
        val tags: Set<DomainEventTag>,
        val batchSize: Int,
        val startOffset: Long?,
        val endOffset: Long?,
        val maxOffset: Long)

data class StreamEvent(
        val offset: Long,
        val id: EventId,
        val aggregateType: String,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val eventType: String,
        val eventTag: DomainEventTag,
        val timestamp: Instant,
        val sequenceNumber: Long,
        val serialisedPayload: String,
        val payloadContentType: SerialisationContentType)

enum class SerialisationContentType(private val value: String) {

    JSON("application/json");

    fun value(): String {
        return value
    }
}

interface EventPayloadMapper {

    fun <E : DomainEvent> deserialiseEvent(serialisedPayload: String, serialisedEventType: String, serialisedEventVersion: Int): E

    fun <E : DomainEvent> serialiseEvent(event: E): PayloadSerialisationResult
}

data class PayloadSerialisationResult(val payload: String, val contentType: SerialisationContentType, val version: Int)

open class MappingException : RuntimeException {

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}
