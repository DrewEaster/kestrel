package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import com.google.gson.JsonObject
import io.ktor.client.HttpClient

abstract class BoundedContextHttpEventStreamSourceFactory(val name: BoundedContextName) {

    protected abstract val mappers: EventMappers

    fun createHttpEventStreamSource(
            httpClient: HttpClient,
            configuration: BoundedContextHttpEventStreamSourceConfiguration,
            offsetManager: OffsetManager,
            jobManager: JobManager): BoundedContextHttpEventStreamSource {

        return BoundedContextHttpEventStreamSource(
                httpClient = httpClient,
                configuration = configuration,
                jobManager = jobManager,
                offsetManager = offsetManager,
                eventMappers = mappers.build()
        )
    }

    class EventMappers {

        val mappersList: MutableList<HttpJsonEventMapper<*>> = mutableListOf()

        fun tag(tagName: String, init: Tag.() -> Unit): Tag {
            val tag = Tag(DomainEventTag(tagName), mappersList)
            tag.init()
            return tag
        }

        fun build(): List<HttpJsonEventMapper<*>> = mappersList
    }

    class Tag(val tag: DomainEventTag, val mappersList: MutableList<HttpJsonEventMapper<*>>) {
        inline fun <reified E: DomainEvent> event(sourceEventType: String, noinline handler: (JsonObject) -> E) {
            mappersList.add(HttpJsonEventMapper(
                sourceEventType = sourceEventType,
                sourceEventTag = tag,
                targetEventClass = E::class,
                map = handler
            ))
        }
    }

    fun eventMappers(init: EventMappers.() -> Unit): EventMappers {
        val mappers = EventMappers()
        mappers.init()
        return mappers
    }
}