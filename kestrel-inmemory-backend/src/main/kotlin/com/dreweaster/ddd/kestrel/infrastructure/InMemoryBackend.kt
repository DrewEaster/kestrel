package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CausationId
import com.dreweaster.ddd.kestrel.application.CorrelationId
import com.dreweaster.ddd.kestrel.application.EventId
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.UnsupportedOperationException
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass

open class InMemoryBackend : Backend {

    private var nextOffset: Long = 0L

    private var events: List<Pair<*, *>> = emptyList()

    fun clear() {
        events = emptyList()
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId): Flux<PersistedEvent<E>> {
        return Flux.fromIterable(persistedEventsFor(aggregateType, aggregateId))
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long): Flux<PersistedEvent<E>> {
        return Flux.fromIterable(persistedEventsFor(aggregateType, aggregateId).filter { it.sequenceNumber > afterSequenceNumber })
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> saveEvents(aggregateType: A, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): Flux<PersistedEvent<E>> {
        if (aggregateHasBeenModified(aggregateType, aggregateId, expectedSequenceNumber)) {
           return Flux.error(OptimisticConcurrencyException)
        }

        val persistedEvents = rawEvents.fold(Pair<Long, List<PersistedEvent<E>>>(expectedSequenceNumber + 1, emptyList())) { acc, e ->
            Pair(acc.first + 1, acc.second +
                PersistedEvent(
                    EventId(UUID.randomUUID().toString()),
                    aggregateType,
                    aggregateId,
                    causationId,
                    correlationId,
                    e::class as KClass<E>,
                    1,
                    e,
                    Instant.now(),
                    acc.first
                )
            )
        }.second

        persistedEvents.forEach { event ->
            events += Pair(event, nextOffset)
            nextOffset += 1
        }

        return Flux.fromIterable(persistedEvents)
    }

    override fun <E : DomainEvent> loadEventStream(tags: Set<DomainEventTag>, afterOffset: Long, batchSize: Int): Mono<EventStream> {
        return Mono.error(UnsupportedOperationException())
    }

    override fun <E : DomainEvent> loadEventStream(tags: Set<DomainEventTag>, afterInstant: Instant, batchSize: Int): Mono<EventStream> {
        return Mono.error(UnsupportedOperationException())
    }

    @Suppress("UNCHECKED_CAST")
    private fun <E : DomainEvent> persistedEventsFor(
            aggregateType: Aggregate<*, E, *>,
            aggregateId: AggregateId): List<PersistedEvent<E>> {
        return events.filter { e ->
            val event = e as Pair<PersistedEvent<E>, Long>
            event.first.aggregateType == aggregateType && event.first.aggregateId == aggregateId
        }.map { event -> event.first as PersistedEvent<E> }
    }

    private fun <E : DomainEvent> aggregateHasBeenModified(
            aggregateType: Aggregate<*, E, *>,
            aggregateId: AggregateId,
            expectedSequenceNumber: Long?): Boolean {

        return persistedEventsFor(aggregateType, aggregateId)
                .lastOrNull()?.sequenceNumber?.equals(expectedSequenceNumber)?.not() == true
    }
}