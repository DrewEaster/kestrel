package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import io.reactivex.Single
import reactor.core.publisher.Mono
import java.time.Instant
import kotlin.reflect.KClass

interface Backend {

    fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId): Single<List<PersistedEvent<E>>>

    fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            afterSequenceNumber: Long): Single<List<PersistedEvent<E>>>

    fun <E : DomainEvent, A: Aggregate<*,E,*>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId? = null): Single<List<PersistedEvent<E>>>

    fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int): Single<EventStream>

    fun <E : DomainEvent> loadEventStream(
            tags: Set<DomainEventTag>,
            afterInstant: Instant,
            batchSize: Int): Single<EventStream>
}

object OptimisticConcurrencyException : RuntimeException()

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