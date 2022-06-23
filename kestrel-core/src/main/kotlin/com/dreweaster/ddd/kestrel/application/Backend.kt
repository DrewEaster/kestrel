package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import kotlin.reflect.KClass

// TODO: Introduce different backend interfaces - AggregateRootBackend and ProcessManagerBackend
interface Backend {

    fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId): Flux<PersistedEvent<E>>

    fun <E : DomainEvent, A: Aggregate<*,E,*>> loadEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            afterSequenceNumber: Long,
            toSequenceNumber: Long? = null): Flux<PersistedEvent<E>>

    fun <S : AggregateState, A: Aggregate<*,*,S>> loadSnapshot(
            aggregateType: A,
            aggregateId: AggregateId): Mono<Snapshot<S>>

    fun <E : DomainEvent, S: AggregateState, A: Aggregate<*,E,S>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId? = null,
            snapshot: Snapshot<S>? = null): Flux<PersistedEvent<E>>

    fun <E : DomainEvent> fetchEventFeed(afterOffset: Long, batchSize: Int): Mono<EventFeed>

    fun <E : DomainEvent> fetchEventFeed(afterInstant: Instant, batchSize: Int): Mono<EventFeed>

    fun <E : DomainEvent> fetchEventFeed(
            tags: Set<DomainEventTag>,
            afterOffset: Long,
            batchSize: Int): Mono<EventFeed>

    fun <E : DomainEvent> fetchEventFeed(
            tags: Set<DomainEventTag>,
            afterInstant: Instant,
            batchSize: Int): Mono<EventFeed>
}

object OptimisticConcurrencyException : RuntimeException()
data class UnexpectedNumberOfRowsAffectedInUpdate(val expected: Int, val actual: Int) : RuntimeException()

data class Snapshot<S: AggregateState>(val version: Long, val state: S?, val causationIdHistory: List<CausationId>)

data class PersistedEvent<E : DomainEvent>(
        val id: EventId,
        val aggregateType: Aggregate<*, E, *>,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val eventType: KClass<out E>,
        val eventVersion: Int,
        val rawEvent: E,
        val timestamp: Instant,
        val sequenceNumber: Long)

data class EventFeed(
        val events: List<FeedEvent>,
        val tags: Set<DomainEventTag>,
        val pageSize: Int,
        val pageStartOffset: Long?,
        val pageEndOffset: Long?,
        val queryMaxOffset: Long,
        val globalMaxOffset: Long)

data class FeedEvent(
        val offset: Long,
        val id: EventId,
        val aggregateType: String,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val eventType: String,
        val eventTag: DomainEventTag,
        val eventVersion: Int,
        val timestamp: Instant,
        val sequenceNumber: Long,
        val serialisedPayload: String)

enum class SerialisationContentType(private val value: String) {

    JSON("application/json");

    fun value(): String {
        return value
    }
}

interface PersistableMappingContext {

    fun <Data : Persistable> deserialise(serialisedPayload: String, serialisedType: String, serialisedVersion: Int): Data

    fun <Data: Persistable> serialise(data: Data): PersistableSerialisationResult
}

data class PersistableSerialisationResult(val payload: String, val contentType: SerialisationContentType, val version: Int)

open class MappingException : RuntimeException {

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}