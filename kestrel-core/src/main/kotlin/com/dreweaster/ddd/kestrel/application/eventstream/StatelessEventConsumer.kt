package com.dreweaster.ddd.kestrel.application.eventstream

import com.dreweaster.ddd.kestrel.domain.DomainEvent

abstract class StatelessEventConsumer(val boundedContexts: BoundedContextEventStreamSources) {

    data class SubscriberConfiguration(
            val name: String,
            val boundedContextName: BoundedContextName,
            val edenPolicy: EventStreamSubscriptionEdenPolicy,
            val eventHandlersBuilder: BoundedContextEventStreamSource.EventHandlersBuilder)

    inner class StatelessEventConsumerBehaviour {

        private var subscribers: List<SubscriberConfiguration> = emptyList()

        fun start() {
            subscribers.forEach { subscriber ->
                boundedContexts[subscriber.boundedContextName]?.subscribe(
                        subscriber.eventHandlersBuilder.build(),
                        EventStreamSubscriberConfiguration(subscriber.name, subscriber.edenPolicy))
            }
        }

        fun subscribe(context: BoundedContextName, subscriptionName: String, edenPolicy: EventStreamSubscriptionEdenPolicy, init: Subscriber.() -> Unit): Subscriber {
            val eventHandlersBuilder = BoundedContextEventStreamSource.EventHandlersBuilder()
            subscribers += SubscriberConfiguration(subscriptionName, context, edenPolicy, eventHandlersBuilder)
            val subscriber = Subscriber(eventHandlersBuilder)
            subscriber.init()
            return subscriber
        }
    }

    class Subscriber(val eventHandlersBuilder: BoundedContextEventStreamSource.EventHandlersBuilder) {

        inline fun <reified E: DomainEvent> event(noinline handler: suspend (E, EventMetadata) -> Unit) {
            eventHandlersBuilder.withHandler(E::class, handler)
        }
    }

    fun consumer(init: StatelessEventConsumerBehaviour.() -> Unit): StatelessEventConsumerBehaviour {
        val behaviour = StatelessEventConsumerBehaviour()
        behaviour.init()
        return behaviour
    }
}