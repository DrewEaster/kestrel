package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer

import com.dreweaster.ddd.kestrel.application.BoundedContextName
import com.dreweaster.ddd.kestrel.application.scheduling.Scheduler
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.application.offset.OffsetTracker
import reactor.netty.http.client.HttpClient
import kotlin.reflect.KClass

abstract class BoundedContextHttpEventSourceFactory(val name: BoundedContextName) {

    protected abstract val deserialisers: EventDeserializers

    fun createHttpEventSource(
            httpClient: HttpClient,
            configuration: BoundedContextHttpEventSourceConfiguration,
            offsetTracker: OffsetTracker,
            jobManager: Scheduler): BoundedContextHttpEventSource {

        return BoundedContextHttpEventSource(
            name = name,
            httpClient = httpClient,
            configuration = configuration,
            jobManager = jobManager,
            offsetTracker = offsetTracker,
            eventMappers = deserialisers.build()
        )
    }

    class EventDeserializers {

        private val deserializersList: MutableList<EventMapper<*>> = mutableListOf()

        fun tag(tagName: String, init: Tag.() -> Unit): Tag {
            val tag = Tag(DomainEventTag(tagName), deserializersList)
            tag.init()
            return tag
        }

        fun build(): List<EventMapper<*>> = deserializersList
    }

    class Tag(val tag: DomainEventTag, val deserializersList: MutableList<EventMapper<*>>) {

        inline fun <E: DomainEvent> event(eventClass: KClass<E>, init: Deserialisers<E>.() -> Unit): Deserialisers<E> {
            val deserialisers = Deserialisers(tag, eventClass, deserializersList)
            deserialisers.init()
            return deserialisers
        }

        inline fun <reified E: DomainEvent> event(init: Deserialisers<E>.() -> Unit): Deserialisers<E> {
            val deserialisers = Deserialisers(tag, E :: class, deserializersList)
            deserialisers.init()
            return deserialisers
        }
    }

    class Deserialisers<E: DomainEvent>(val tag: DomainEventTag, val clazz: KClass<E>, val deserializersList: MutableList<EventMapper<*>>) {

        inline fun deserialiser(type: String, version: Int, noinline handler: (String, String, Int) -> E) {
            deserializersList.add(EventMapper(
                sourceEventType = type,
                sourceEventTag = tag,
                sourceEventVersion = version,
                targetEventClass = clazz,
                map = handler
            ))
        }
    }

    fun eventDeserialisers(init: EventDeserializers.() -> Unit): EventDeserializers {
        val deserializers = EventDeserializers()
        deserializers.init()
        return deserializers
    }
}