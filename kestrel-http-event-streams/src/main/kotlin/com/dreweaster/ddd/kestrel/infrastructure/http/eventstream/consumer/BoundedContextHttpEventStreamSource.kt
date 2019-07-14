package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer

import com.dreweaster.ddd.kestrel.application.eventstream.*
import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.CausationId
import com.dreweaster.ddd.kestrel.application.CorrelationId
import com.dreweaster.ddd.kestrel.application.EventId
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.BoundedContextHttpEventStreamSourceReporter
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.ReportingContext
import com.github.salomonbrys.kotson.long
import com.github.salomonbrys.kotson.nullString
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.url
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
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

// TODO: Need to factor skipped events into batch size - i.e. always event minimum of batch size even if that means fetching multiple batches
class BoundedContextHttpEventStreamSource(
        val httpClient: HttpClient,
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
                val jsonBody = request(httpClient)
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
        fun createRequest(lastProcessedOffset: Long?): suspend (HttpClient) -> JsonObject
    }
}

object BeginningOfTime : HttpEventStreamSubscriptionEdenPolicy() {
    override fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventStreamSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory {

        return object : RequestFactory {
            override fun createRequest(lastProcessedOffset: Long?): suspend (HttpClient) -> JsonObject {
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

                return { client: HttpClient ->
                    client.get {
                        url(url)
                    }
                }
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
            override fun createRequest(lastProcessedOffset: Long?): suspend (HttpClient) -> JsonObject {
                if(lastProcessedOffset != null) {
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

                    return { client: HttpClient ->
                        client.get {
                            url(url)
                        }
                    }
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

                    return { client: HttpClient ->
                        client.get {
                            url(url)
                        }
                    }
                }
            }
        }
    }
}