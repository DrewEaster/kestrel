package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CausationId
import com.dreweaster.ddd.kestrel.application.CorrelationId
import com.dreweaster.ddd.kestrel.application.EventId
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
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

    private var events: List<Pair<PersistedEvent<*>, Long>> = emptyList()

    private var snapshots: Map<AggregateId, PersistedSnapshot<*,*,*>> = emptyMap()

    fun countAllEvents(): Int {
        return events.size
    }

    fun clear() {
        clearEvents()
        clearSnapshots()
        nextOffset = 0
    }

    fun clearEvents(filter: (Pair<PersistedEvent<*>,Long>) -> Boolean = { true } ) {
        events = events.filterNot(filter)
        nextOffset = events.lastOrNull()?.let { it.second + 1 } ?: 0
    }

    fun clearSnapshots() {
        snapshots = emptyMap()
    }

    override fun <S : AggregateState, A : Aggregate<*, *, S>> loadSnapshot(aggregateType: A, aggregateId: AggregateId): Mono<Snapshot<S>> {
        return snapshots[aggregateId]?.let { Mono.just(it.snapshot as Snapshot<S>)  } ?: Mono.empty()
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId): Flux<PersistedEvent<E>> {
        return Flux.fromIterable(persistedEventsFor(aggregateType, aggregateId))
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long, toSequenceNumber: Long?): Flux<PersistedEvent<E>> {
        return Flux.fromIterable(persistedEventsFor(aggregateType, aggregateId).filter { it.sequenceNumber > afterSequenceNumber && toSequenceNumber?.let { to -> it.sequenceNumber <= to } ?: true })
    }

    override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId?,
            snapshot: Snapshot<S>?): Flux<PersistedEvent<E>> {

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

        snapshot?.let {
            snapshots += (aggregateId to PersistedSnapshot(aggregateId, aggregateType, it))
        }

        return Flux.fromIterable(persistedEvents)
    }

    override fun <E : DomainEvent> fetchEventFeed(tags: Set<DomainEventTag>, afterOffset: Long, batchSize: Int): Mono<EventFeed> {
        return Mono.error(UnsupportedOperationException())
    }

    override fun <E : DomainEvent> fetchEventFeed(tags: Set<DomainEventTag>, afterInstant: Instant, batchSize: Int): Mono<EventFeed> {
        return Mono.error(UnsupportedOperationException())
    }

    override fun <E : DomainEvent> fetchEventFeed(afterOffset: Long, batchSize: Int): Mono<EventFeed> {
        return Mono.error(UnsupportedOperationException())
    }

    override fun <E : DomainEvent> fetchEventFeed(afterInstant: Instant, batchSize: Int): Mono<EventFeed> {
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

    data class PersistedSnapshot<E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>>(
            val aggregateId: AggregateId,
            val aggregateType: A,
            val snapshot: Snapshot<S>)
}