package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.eventstream.*
import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.cluster.LocalClusterManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.InMemoryOffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.BoundedContextHttpEventStreamSourceProbe
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.BoundedContextHttpEventStreamSourceReporter
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.ReportingContext
import com.dreweaster.ddd.kestrel.infrastructure.job.ScheduledExecutorServiceJobManager
import com.github.salomonbrys.kotson.get
import com.github.salomonbrys.kotson.long
import com.github.salomonbrys.kotson.nullString
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.experimental.CancellableContinuation
import kotlinx.coroutines.experimental.suspendCancellableCoroutine
import org.asynchttpclient.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.reflect.KClass

typealias FullyQualifiedClassName = String

data class HttpJsonEventMapper<T: DomainEvent>(
    val targetEventClass: KClass<T>,
    val sourceEventTag: DomainEventTag,
    val sourceEventType: FullyQualifiedClassName,
    val map: (JsonObject) -> T)

interface BoundedContextHttpEventStreamSourceConfiguration {

    val producerEndpointProtocol: String

    val producerEndpointHostname: String

    val producerEndpointPort: Int

    val producerEndpointPath: String

    fun batchSizeFor(subscriptionName: String): Int

    fun repeatScheduleFor(subscriptionName: String): Duration

    fun enabled(subscriptionName: String): Boolean
}

// TODO: Need to factor skipped events into batch size - i.e. always process minimum of batch size even if that means fetching multiple batches
class BoundedContextHttpEventStreamSource(
        val httpClient: AsyncHttpClient,
        val configuration: BoundedContextHttpEventStreamSourceConfiguration,
        eventMappers: List<HttpJsonEventMapper<*>>,
        val offsetManager: OffsetManager,
        private val jobManager: JobManager): BoundedContextEventStreamSource {

    private val LOG = LoggerFactory.getLogger(BoundedContextHttpEventStreamSource::class.java)

    private val targetClassToEventTag: Map<KClass<out DomainEvent>, DomainEventTag> = eventMappers.map { it.targetEventClass to it.sourceEventTag }.toMap()

    private val sourceEventTypeToMapper: Map<FullyQualifiedClassName, (JsonObject) -> DomainEvent> = eventMappers.map { it.sourceEventType to { jsonObject: JsonObject -> it.map(jsonObject)} }.toMap()

    private var reporters: List<BoundedContextHttpEventStreamSourceReporter> = emptyList()

    fun addReporter(reporter: BoundedContextHttpEventStreamSourceReporter): BoundedContextHttpEventStreamSource {
        reporters += reporter
        return this
    }

    fun removeReporter(reporter: BoundedContextHttpEventStreamSourceReporter): BoundedContextHttpEventStreamSource {
        reporters -= reporter
        return this
    }

    override fun subscribe(handlers: Map<KClass<out DomainEvent>, (suspend (DomainEvent, EventMetadata) -> Unit)>, subscriberConfiguration: EventStreamSubscriberConfiguration) {
        val allTags = handlers.keys.map { targetClassToEventTag[it] ?: throw IllegalArgumentException("Unsupported event type: ${it.qualifiedName}") }.toSet()

        val job = ConsumeHttpEventStreamJob(
                tags = allTags,
                subscriberConfiguration = subscriberConfiguration,
                eventHandlers = handlers)

        if(configuration.enabled(subscriberConfiguration.name)) {
            jobManager.scheduleManyTimes(configuration.repeatScheduleFor(subscriberConfiguration.name), job)
        } else {
            LOG.warn("The event stream subscriber '${subscriberConfiguration.name}' is disabled")
        }
    }

    inner class ConsumeHttpEventStreamJob(
            private val eventHandlers: Map<KClass<out DomainEvent>, (suspend (DomainEvent, EventMetadata) -> Unit)>,
            tags : Set<DomainEventTag>,
            subscriberConfiguration: EventStreamSubscriberConfiguration) : Job {

        override val name = subscriberConfiguration.name

        private val probe = ReportingContext(name, reporters)

        private val requestFactory = HttpEventStreamSubscriptionEdenPolicy.from(subscriberConfiguration.edenPolicy)
            .newRequestFactory(
                subscriberConfiguration = configuration,
                tags = tags,
                batchSize = configuration.batchSizeFor(subscriberConfiguration.name))

        private val jsonParser = JsonParser()

        override suspend fun execute() {
            probe.startedConsuming()
            try {
                val lastProcessedOffset = fetchOffset()
                val events = fetchEvents(lastProcessedOffset)

                events.forEach { event ->
                    val eventOffset = event["offset"].long
                    handleEvent(event)
                    saveOffset(eventOffset)
                }

                probe.finishedConsuming()
            } catch (ex: Exception) {
                probe.finishedConsuming(ex)
                throw ex
            }
        }

        private suspend fun handleEvent(eventJson: JsonObject) {
            val eventType = eventJson["type"].string
            probe.startedHandlingEvent(eventType)
            try {
                sourceEventTypeToMapper[eventType]?.let { mapper ->
                    val event = mapper(eventJson["payload"].asJsonObject)
                    val eventHandler = eventHandlers[event::class]
                    eventHandler?.invoke(event, extractEventMetadata(eventJson))
                }
                probe.finishedHandlingEvent()
            } catch(ex: Exception) {
                probe.finishedHandlingEvent(ex)
                throw ex
            }
        }

        private suspend fun saveOffset(offset: Long) {
            probe.startedSavingOffset()
            try {
                offsetManager.saveOffset(name, offset)
                probe.finishedSavingOffset(offset)
            } catch(ex: Exception) {
                probe.finishedSavingOffset(ex)
                throw ex
            }
        }

        private suspend fun fetchOffset(): Long? {
            probe.startedFetchingOffset()
            return try {
                val offset = offsetManager.getOffset(name)
                probe.finishedFetchingOffset()
                offset
            } catch (ex: Exception) {
                probe.finishedFetchingOffset(ex)
                throw ex
            }
        }

        private suspend fun fetchEvents(lastProcessedOffset: Long?): List<JsonObject> {
            probe.startedFetchingEventStream()
            val request = requestFactory.createRequest(lastProcessedOffset)
            return try {
                val response = httpClient.execute(request)
                val jsonBody = jsonParser.parse(response.responseBody)
                val maxOffset = jsonBody["max_offset"].asLong
                probe.finishedFetchingEventStream(maxOffset)
                jsonBody["events"].asJsonArray.toList().map { it.asJsonObject }
            } catch(ex: Exception) {
                probe.finishedFetchingEventStream(ex)
                throw ex
            }
        }

        private fun extractEventMetadata(eventJson: JsonObject) =
            EventMetadata(
                EventId(eventJson["id"].string),
                AggregateId(eventJson["aggregate_id"].string),
                CausationId(eventJson["causation_id"].string),
                eventJson["correlation_id"].nullString?.let { CorrelationId(it) },
                eventJson["sequence_number"].long
            )

        private suspend fun AsyncHttpClient.execute(request: Request): Response {
            return suspendCancellableCoroutine { cont: CancellableContinuation<Response> ->
                this.executeRequest(request, object : AsyncCompletionHandler<Unit>() {
                    override fun onCompleted(response: Response) {
                        cont.resume(response)
                    }

                    override fun onThrowable(ex: Throwable) {
                        cont.resumeWithException(ex)
                    }
                })
                Unit
            }
        }
    }


}

sealed class HttpEventStreamSubscriptionEdenPolicy {

    companion object {
        fun from(policy: EventStreamSubscriptionEdenPolicy) = when(policy) {
            EventStreamSubscriptionEdenPolicy.FROM_NOW -> FromNow
            EventStreamSubscriptionEdenPolicy.BEGINNING_OF_TIME -> BeginningOfTime
        }
    }

    abstract fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventStreamSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory

    interface RequestFactory {
        fun createRequest(lastProcessedOffset: Long?): Request
    }
}

object BeginningOfTime : HttpEventStreamSubscriptionEdenPolicy() {
    override fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventStreamSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory {

        return object : RequestFactory {
            override fun createRequest(lastProcessedOffset: Long?): Request {
                val query = HttpJsonEventQuery(
                        tags = tags,
                        afterOffset = lastProcessedOffset ?: -1L,
                        batchSize = batchSize
                )

                val url = query.eventsUrlFor(
                    protocol = subscriberConfiguration.producerEndpointProtocol,
                    hostname = subscriberConfiguration.producerEndpointHostname,
                    port = subscriberConfiguration.producerEndpointPort,
                    path = subscriberConfiguration.producerEndpointPath
                )

                return RequestBuilder().setUrl(url.toString()).setMethod("GET").build()
            }
        }
    }
}

object FromNow : HttpEventStreamSubscriptionEdenPolicy() {
    override fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventStreamSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory {

        val now = Instant.now() // cache now() once so doesn't refresh on every request

        return object : RequestFactory {
            override fun createRequest(lastProcessedOffset: Long?): Request {
                return if(lastProcessedOffset != null) {
                    val query = HttpJsonEventQuery(
                            tags = tags,
                            afterOffset = lastProcessedOffset,
                            batchSize = batchSize
                    )

                    val url = query.eventsUrlFor(
                        protocol = subscriberConfiguration.producerEndpointProtocol,
                        hostname = subscriberConfiguration.producerEndpointHostname,
                        port = subscriberConfiguration.producerEndpointPort,
                        path = subscriberConfiguration.producerEndpointPath
                    )

                    RequestBuilder().setUrl(url.toString()).setMethod("GET").build()
                } else {
                    val query = HttpJsonEventQuery(
                            tags = tags,
                            afterTimestamp = now,
                            batchSize = batchSize
                    )

                    val url = query.eventsUrlFor(
                        protocol = subscriberConfiguration.producerEndpointProtocol,
                        hostname = subscriberConfiguration.producerEndpointHostname,
                        port = subscriberConfiguration.producerEndpointPort,
                        path = subscriberConfiguration.producerEndpointPath
                    )

                    RequestBuilder().setUrl(url.toString()).setMethod("GET").build()
                }
            }
        }
    }
}