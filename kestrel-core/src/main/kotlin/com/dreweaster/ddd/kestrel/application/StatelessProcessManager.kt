package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import reactor.core.publisher.Mono

abstract class StatelessProcessManager(val boundedContexts: BoundedContextEventSources) {

    data class SubscriberConfiguration(
            val name: String,
            val boundedContextName: BoundedContextName,
            val edenPolicy: BoundedContextSubscriptionEdenPolicy,
            val eventHandlersBuilder: BoundedContextEventSource.EventHandlersBuilder)

    inner class StatelessProcessManagerBehaviour(private val name: String) {

        private var subscribers: List<SubscriberConfiguration> = emptyList()

        fun start() {
            subscribers.forEach { subscriber ->
                boundedContexts[subscriber.boundedContextName]?.subscribe(
                    subscriber.eventHandlersBuilder.build(),
                        BoundedContextSubscriberConfiguration("${name}_${subscriber.name}", subscriber.edenPolicy))
            }
        }

        fun subscribe(name: String, context: BoundedContextName, edenPolicy: BoundedContextSubscriptionEdenPolicy, init: Subscriber.() -> Unit): Subscriber {
            val eventHandlersBuilder = BoundedContextEventSource.EventHandlersBuilder()
            subscribers += SubscriberConfiguration(name, context, edenPolicy, eventHandlersBuilder)
            val subscriber = Subscriber(eventHandlersBuilder)
            subscriber.init()
            return subscriber
        }
    }

    class Subscriber(val eventHandlersBuilder: BoundedContextEventSource.EventHandlersBuilder) {

        inline fun <reified E: DomainEvent> event(noinline handler: (E, EventMetadata) -> Mono<Void>) {
            eventHandlersBuilder.withHandler(E::class, handler)
        }
    }

    fun processManager(name: String, init: StatelessProcessManagerBehaviour.() -> Unit): StatelessProcessManagerBehaviour {
        val behaviour = StatelessProcessManagerBehaviour(name)
        behaviour.init()
        return behaviour
    }
}