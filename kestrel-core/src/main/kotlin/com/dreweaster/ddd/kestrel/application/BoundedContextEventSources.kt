package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import reactor.core.publisher.Mono
import java.lang.RuntimeException
import kotlin.reflect.KClass

interface BoundedContextName { val name: String }

class BoundedContextEventSources(sources: List<Pair<BoundedContextName, BoundedContextEventSource>>) {

    private val sourcesMap = sources.toMap()

    operator fun get(name: BoundedContextName) = sourcesMap[name]
}

interface BoundedContextEventSource {

    class EventHandlersBuilder {

        var handlers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (E, EventMetadata) -> Mono<Void>): EventHandlersBuilder {
            handlers += type to handler as (DomainEvent, EventMetadata) -> Mono<Void>
            return this
        }

        fun build() = handlers
    }

    fun subscribe(handlers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>>, subscriberConfiguration: BoundedContextSubscriberConfiguration)
}

data class BoundedContextSubscriberConfiguration(val name: String, val edenPolicy: BoundedContextSubscriptionEdenPolicy)

data class EventMetadata(
        val eventId: EventId,
        val aggregateId: AggregateId,
        val causationId: CausationId,
        val correlationId: CorrelationId?,
        val sequenceNumber: Long)

enum class BoundedContextSubscriptionEdenPolicy { BEGINNING_OF_TIME, FROM_NOW }