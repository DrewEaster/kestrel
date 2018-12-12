package com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.eventstream.EventMetadata
import com.dreweaster.ddd.kestrel.application.eventstream.StatelessEventConsumer
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSource
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.HttpJsonEventMapper
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import com.google.gson.JsonObject
import org.asynchttpclient.AsyncHttpClient

abstract class BoundedContextHttpEventStreamSourceFactory(val name: BoundedContextName) {

    protected abstract val mappers: EventMappers

    fun createHttpEventStreamSource(
            httpClient: AsyncHttpClient,
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

        var mappersList: List<HttpJsonEventMapper<*>> = emptyList()

        fun tag(tagName: String, init: Tag.() -> Unit): Tag {
            val tag = Tag(DomainEventTag(tagName))
            tag.init()
            return tag
        }

        fun build(): List<HttpJsonEventMapper<*>> = mappersList

        inner class Tag(val tag: DomainEventTag) {
            inline fun <reified E: DomainEvent> event(sourceEventType: String, noinline handler: (JsonObject) -> E) {
                mappersList += HttpJsonEventMapper(
                    sourceEventType = sourceEventType,
                    sourceEventTag = tag,
                    targetEventClass = E::class,
                    map = handler
                )
            }
        }
    }

    fun eventMappers(init: EventMappers.() -> Unit): EventMappers {
        val mappers = EventMappers()
        mappers.init()
        return mappers
    }
}