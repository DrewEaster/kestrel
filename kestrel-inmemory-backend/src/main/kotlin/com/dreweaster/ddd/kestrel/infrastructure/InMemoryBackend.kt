package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import kotlinx.coroutines.delay
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass

open class InMemoryBackend : Backend {

    private var nextOffset: Long = 0L

    private var events: List<Pair<*, *>> = emptyList()

    fun clear() {
        events = emptyList()
    }

    suspend override fun <E : DomainEvent> loadEvents(
            aggregate: Aggregate<*, E, *>,
            aggregateId: AggregateId): List<PersistedEvent<E>> {
        return persistedEventsFor(aggregate, aggregateId)
    }

    suspend override fun <E : DomainEvent> loadEvents(
            aggregateType: Aggregate<*, E, *>,
            aggregateId: AggregateId,
            afterSequenceNumber: Long): List<PersistedEvent<E>> {
        return persistedEventsFor(aggregateType, aggregateId).filter { it.sequenceNumber > afterSequenceNumber }
    }

    suspend override fun <E : DomainEvent> loadEventStream(
            tag: DomainEventTag,
            afterOffset: Long,
            batchSize: Int): EventStream {
        throw UnsupportedOperationException()
    }

    suspend override fun <E : DomainEvent> loadEventStream(
            tag: DomainEventTag,
            afterInstant: Instant,
            batchSize: Int): EventStream {
        throw UnsupportedOperationException()
    }

    @Suppress("UNCHECKED_CAST")
    suspend override fun <E : DomainEvent> saveEvents(
            aggregateType: Aggregate<*, E, *>,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId?): List<PersistedEvent<E>> {

        if (aggregateHasBeenModified(aggregateType, aggregateId, expectedSequenceNumber)) {
            throw OptimisticConcurrencyException
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
                    acc.first)
            )
        }.second

        persistedEvents.forEach { event ->
            events += Pair(event, nextOffset)
            nextOffset += 1
        }

        return persistedEvents
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