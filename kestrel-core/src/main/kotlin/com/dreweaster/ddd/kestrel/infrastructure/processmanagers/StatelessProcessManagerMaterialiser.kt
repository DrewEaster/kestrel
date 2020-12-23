package com.dreweaster.ddd.kestrel.infrastructure.processmanagers

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextName
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriberConfiguration
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy
import com.dreweaster.ddd.kestrel.application.EventMetadata
import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.lang.RuntimeException
import kotlin.reflect.KClass

class UnhandledProcessManagerEvent(event: DomainEvent): RuntimeException("The process manager's domain behaviour does not handle the event ${event::class.java}")

abstract class StatelessProcessManagerMaterialiser(val boundedContexts: BoundedContextEventSources) {

    private val log = LoggerFactory.getLogger(StatelessProcessManagerMaterialiser::class.java)

    class EventMappersBuilder {

        var mappers: Map<KClass<out DomainEvent>, (DomainEvent, EventMetadata) -> Mono<Pair<DomainEvent, EventMetadata>>> = emptyMap()

        fun <E: DomainEvent> withMapper(type: KClass<E>, handler: (E, EventMetadata) -> Mono<Pair<DomainEvent, EventMetadata>>): EventMappersBuilder {
            mappers += type to handler as (DomainEvent, EventMetadata) -> Mono<Pair<DomainEvent, EventMetadata>>
            return this
        }
    }

    data class SubscriberConfiguration(
            val name: String,
            val boundedContextName: BoundedContextName,
            val edenPolicy: BoundedContextSubscriptionEdenPolicy,
            val eventMappersBuilder: EventMappersBuilder)


    class Subscriber(val eventMappersBuilder: EventMappersBuilder) {

        inline fun <reified E: DomainEvent> event(noinline handler: (E, EventMetadata) -> Mono<Pair<DomainEvent, EventMetadata>>) {
            eventMappersBuilder.withMapper(E::class, handler)
        }

        inline fun <reified E: DomainEvent> event() {
            eventMappersBuilder.withMapper(E::class) { evt, metadata -> Mono.just(evt to metadata) }
        }
    }

    inner class Behaviour(private val processManager: StatelessProcessManager) {

        private var subscribers: List<SubscriberConfiguration> = emptyList()

        fun start() {
            subscribers.forEach { subscriber ->
                boundedContexts[subscriber.boundedContextName]?.subscribe(
                    subscriber.eventMappersBuilder.mappers.mapValues { (_, mapper) -> { sourceEvent, sourceMetadata ->
                        mapper(sourceEvent, sourceMetadata).flatMap { (mappedEvent, mappedMetadata) ->
                            when (mappedEvent) {
                                sourceEvent -> log.info("Source event ${sourceEvent::class} was not mapped")
                                else -> log.info("Source event ${sourceEvent::class} was mapped to ${mappedEvent::class.java}")
                            }
                            processManager.behaviour.second[mappedEvent::class]?.let { it(mappedEvent, mappedMetadata) } ?: Mono.error(UnhandledProcessManagerEvent(mappedEvent)
                           )
                        }
                    }},
                    BoundedContextSubscriberConfiguration("${processManager.behaviour.first}_${subscriber.name}", subscriber.edenPolicy)
                )
            }
        }

        fun subscribe(name: String, context: BoundedContextName, edenPolicy: BoundedContextSubscriptionEdenPolicy, init: Subscriber.() -> Unit): Subscriber {
            val eventMappersBuilder = EventMappersBuilder()
            subscribers += SubscriberConfiguration(name, context, edenPolicy, eventMappersBuilder)
            val subscriber = Subscriber(eventMappersBuilder)
            subscriber.init()
            return subscriber
        }
    }

    fun materialise(processManager: StatelessProcessManager, init: Behaviour.() -> Unit): Behaviour {
        val behaviour = Behaviour(processManager)
        behaviour.init()
        return behaviour
    }
}