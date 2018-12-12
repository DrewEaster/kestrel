package com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSource
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.HttpJsonEventMapper
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import org.asynchttpclient.AsyncHttpClient

abstract class BoundedContextHttpEventStreamSourceFactory(val name: BoundedContextName) {

    abstract val mappers: List<HttpJsonEventMapper<*>>

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
                eventMappers = mappers
        )
    }
}