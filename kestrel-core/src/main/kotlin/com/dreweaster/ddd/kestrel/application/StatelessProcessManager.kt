package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import reactor.core.publisher.Mono
import kotlin.reflect.KClass

interface StatelessProcessManager {

    class EventHandlersBuilder {

        var handlers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (E, EventMetadata) -> Mono<Void>): EventHandlersBuilder {
            handlers += type to handler as (DomainEvent, EventMetadata) -> Mono<Void>
            return this
        }
    }

    class Behaviour {

        val eventHandlersBuilder = EventHandlersBuilder()

        inline fun <reified E: DomainEvent> event(noinline handler: (E, EventMetadata) -> Mono<Void>) {
            eventHandlersBuilder.withHandler(E::class, handler)
        }

        fun handlers(): Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>> = eventHandlersBuilder.handlers
    }

    val behaviour: Pair<String, Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>>>

    fun processManager(name: String, init: Behaviour.() -> Unit): Pair<String, Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Void>>> {
        val behaviour = Behaviour()
        behaviour.init()
        return name to behaviour.handlers()
    }
}
