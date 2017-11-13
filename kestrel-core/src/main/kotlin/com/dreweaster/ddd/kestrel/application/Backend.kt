package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass

interface Backend {

    suspend fun <E : DomainEvent> loadEvents(
            aggregate: Aggregate<*,E,*>,
            aggregateId: AggregateId): List<PersistedEvent<E>>

    suspend fun <E : DomainEvent> loadEvents(
            aggregateType: Aggregate<*,E,*>,
            aggregateId: AggregateId,
            afterSequenceNumber: Long): List<PersistedEvent<E>>

    suspend fun <E : DomainEvent> loadEventStream(
            tag: DomainEventTag,
            afterOffset: Long?,
            batchSize: Int): List<StreamEvent>

    suspend fun <E : DomainEvent> loadEventStream(
            tag: DomainEventTag,
            afterInstant: Instant,
            batchSize: Int): List<StreamEvent>

    suspend fun <E : DomainEvent> saveEvents(
            aggregateType: Aggregate<*,E,*>,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId? = null): List<PersistedEvent<E>>
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

data class StreamEvent(
        val offset: Long,
        val id: String,
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

data class PayloadSerialisationResult(val payload: String, val contentType: SerialisationContentType, val version: Optional<Int>)

class MappingException : RuntimeException {

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}
