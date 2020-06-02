package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.scheduling.Job
import com.dreweaster.ddd.kestrel.application.scheduling.Scheduler
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.application.offset.Offset
import com.dreweaster.ddd.kestrel.application.offset.LastProcessedOffset
import com.dreweaster.ddd.kestrel.application.offset.OffsetTracker
import com.dreweaster.ddd.kestrel.util.json.*
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.time.Duration
import java.time.Instant
import kotlin.reflect.KClass

typealias FullyQualifiedClassName = String
typealias SerialisedEventVersion = Int
typealias SerialisedEventPayload = String

data class EventMapper<T: DomainEvent>(
    val targetEventClass: KClass<T>,
    val sourceEventTag: DomainEventTag,
    val sourceEventType: FullyQualifiedClassName,
    val sourceEventVersion: SerialisedEventVersion,
    val map: (SerialisedEventPayload, FullyQualifiedClassName, SerialisedEventVersion) -> T)

data class SourceEvent(val json: ObjectNode) {
    val payload: String by lazy { json["payload"].string }
    val type: String by lazy { json["type"].string }
    val tag: String by lazy { json["tag"].string }
    val version: Int by lazy { json["version"].int }
    val offset: Long by lazy { json["offset"].long }
    val metadata: EventMetadata by lazy {
        EventMetadata(
            EventId(json["id"].string),
            AggregateId(json["aggregate_id"].string),
            CausationId(json["causation_id"].string),
            json["correlation_id"].nullString?.let { CorrelationId(it) },
            json["sequence_number"].long
        )
    }
}

data class EventSourcePage(val json: ObjectNode, val pageSize: Int) {
    val events: List<SourceEvent> by lazy { json["events"].array.toList().map { SourceEvent(it.obj) }}
    val queryMaxOffset: Long by lazy { json["query_max_offset"].long }
    val globalMaxOffset: Long by lazy { json["global_max_offset"].long }
    val pageStartOffset: Long by lazy { json["page_start_offset"].long }
    val pageEndOffset: Long by lazy { json["page_end_offset"].long }
}

class UnrecognisedSourceEventException(error: String, val event: SourceEvent): java.lang.RuntimeException(error)

interface BoundedContextHttpEventSourceConfiguration {

    val producerEndpointProtocol: String

    val producerEndpointHostname: String

    val producerEndpointPort: Int

    val producerEndpointPath: String

    fun batchSizeFor(subscriptionName: String): Int

    fun repeatScheduleFor(subscriptionName: String): Duration

    fun timeoutFor(subscriptionName: String): Duration

    fun enabled(subscriptionName: String): Boolean

    fun ignoreUnrecognisedEvents(subscriptionName: String): Boolean
}

// TODO: Renable monitoring
class BoundedContextHttpEventSource(
        val name: BoundedContextName,
        val httpClient: HttpClient,
        val configuration: BoundedContextHttpEventSourceConfiguration,
        eventMappers: List<EventMapper<*>>,
        val offsetTracker: OffsetTracker,
        private val jobManager: Scheduler): BoundedContextEventSource {

    private val LOG = LoggerFactory.getLogger(BoundedContextHttpEventSource::class.java)

    private val targetClassToEventTag: Map<KClass<out DomainEvent>, DomainEventTag> = eventMappers.map { it.targetEventClass to it.sourceEventTag }.toMap()

    private val sourceEventTypeToMapper: Map<FullyQualifiedClassName, Map<SerialisedEventVersion, (SerialisedEventPayload) -> DomainEvent>> = eventMappers.fold(emptyMap()) { acc, mapper ->
        val deserialisers = acc[mapper.sourceEventType] ?: emptyMap()
        acc + (mapper.sourceEventType to (deserialisers + (mapper.sourceEventVersion to { serialisedPayload: String -> mapper.map(serialisedPayload, mapper.sourceEventType, mapper.sourceEventVersion)})))
    }

    private var reporters: List<BoundedContextHttpEventSourceReporter> = emptyList()

    fun addReporter(reporter: BoundedContextHttpEventSourceReporter): BoundedContextHttpEventSource {
        reporters += reporter
        return this
    }

    fun removeReporter(reporter: BoundedContextHttpEventSourceReporter): BoundedContextHttpEventSource {
        reporters -= reporter
        return this
    }

    override fun subscribe(handlers: Map<KClass<out DomainEvent>, ((DomainEvent, EventMetadata) -> Mono<Void>)>, subscriberConfiguration: BoundedContextSubscriberConfiguration) {
        val allTags = handlers.keys.map { targetClassToEventTag[it] ?: throw IllegalArgumentException("Unsupported event type: ${it.qualifiedName}") }.toSet()

        val job = ConsumeHttpEventSourceJob(
                tags = allTags,
                subscriberConfiguration = subscriberConfiguration,
                eventHandlers = handlers)

        if(configuration.enabled(subscriberConfiguration.name)) {
            jobManager.scheduleManyTimes(
                repeatSchedule = configuration.repeatScheduleFor(subscriberConfiguration.name),
                timeout = configuration.timeoutFor(subscriberConfiguration.name),
                job = job)
        } else {
            LOG.warn("The event stream subscriber '${subscriberConfiguration.name}' is disabled")
        }
    }

    inner class ConsumeHttpEventSourceJob(
            private val eventHandlers: Map<KClass<out DomainEvent>, ((DomainEvent, EventMetadata) -> Mono<Void>)>,
            tags : Set<DomainEventTag>,
            private val subscriberConfiguration: BoundedContextSubscriberConfiguration) : Job {

        override val name = "${this@BoundedContextHttpEventSource.name.name}_${subscriberConfiguration.name}"

        private val probe = ReportingContext(this@BoundedContextHttpEventSource.name, name, reporters)

        private val requestFactory = HttpEventSourceSubscriptionEdenPolicy.from(subscriberConfiguration.edenPolicy)
            .newRequestFactory(
                subscriberConfiguration = configuration,
                tags = tags,
                batchSize = configuration.batchSizeFor(subscriberConfiguration.name))

        // TODO: Arguably a little inefficient in that it updates offset after handling each event rather than in batches. Does reduce volume of redeliveries, though
        override fun execute(): Mono<Boolean> {
            probe.startedConsuming()
            return fetchOffset()
                .flatMap ( fetchEventSourcePage )
                .flatMap { processEvents(it).then(Mono.just(hasBacklog(it.second))) }
                .doOnSuccess { probe.finishedConsuming() }
                .doOnError { probe.finishedConsuming(it) }
        }

        private val handleEvent: (SourceEvent) -> Mono<Void> = { event ->

            fun invokeEventHandler(mapper: (SerialisedEventPayload) -> DomainEvent): Mono<Void> {
                val rawEvent = mapper(event.payload)
                val eventHandler = eventHandlers[rawEvent::class]
                return eventHandler?.invoke(rawEvent, event.metadata) ?: Mono.empty()
            }

            probe.startedHandlingEvent(event)
            val result = when(val mapper = sourceEventTypeToMapper[event.type]?.get(event.version)) {
                null -> {
                    handleUnrecognisedSourceEvent(event)
                        .doOnSuccess { probe.finishedHandlingEvent(ignored = true) }
                        .doOnError { probe.finishedHandlingEvent(it) }
                }
                else -> {
                    invokeEventHandler(mapper)
                        .doOnSuccess { probe.finishedHandlingEvent() }
                        .doOnError { probe.finishedHandlingEvent(it) }
                }
            }

            result.then(Mono.just(event.offset)).flatMap(saveOffset)
        }

        private val saveOffset: (Long) -> Mono<Void> = { offset ->
            probe.startedSavingOffset()
            offsetTracker.saveOffset(name, offset)
                .doOnSuccess { probe.finishedSavingOffset(offset) }
                .doOnError { probe.finishedSavingOffset(it) }
        }

        private fun handleUnrecognisedSourceEvent(event: SourceEvent): Mono<Void> {
            val message = "Unrecognised source event [ offset = ${event.offset}, tag = ${event.tag}, type = ${event.type}, version = ${event.version} ]"
            return when(configuration.ignoreUnrecognisedEvents(subscriberConfiguration.name)) {
                true -> {
                    LOG.warn(message, event)
                    Mono.empty()
                }
                false -> Mono.error(UnrecognisedSourceEventException(message, event))
            }
        }

        private fun fetchOffset(): Mono<out Offset> {
            probe.startedFetchingOffset()
            return offsetTracker.getOffset(name)
                .doOnSuccess { probe.finishedFetchingOffset(it) }
                .doOnError { probe.finishedFetchingOffset(it) }
        }

        private val fetchEventSourcePage: (Offset) -> Mono<Pair<Offset, EventSourcePage>> = { eventSourceOffset ->
            val offset = when(eventSourceOffset) {
                is LastProcessedOffset -> eventSourceOffset.value
                else -> null
            }

            probe.startedFetchingEventSourcePage()
            requestFactory.createRequest(offset)(httpClient).map {
                eventSourceOffset to EventSourcePage(it, configuration.batchSizeFor(subscriberConfiguration.name))
            }.doOnSuccess { probe.finishedFetchingEventSourcePage(it.first, it.second)
            }.doOnError { probe.finishedFetchingEventSourcePage(it) }
        }

        private fun processEvents(page: Pair<Offset, EventSourcePage>): Mono<Void> {
            val (currentOffset, currentPage) = page
            return if(currentPage.events.isEmpty()) {
                val derivedOffset = maxOf(currentPage.queryMaxOffset, currentPage.globalMaxOffset)
                if(configuration.batchSizeFor(subscriberConfiguration.name) > 0 && derivedOffset > -1L && offsetHasChanged(currentOffset, derivedOffset)) Mono.just(derivedOffset).flatMap(saveOffset) else Mono.empty()
            } else {
                Flux.fromIterable(currentPage.events).concatMap(handleEvent).then()
            }
        }

        private fun offsetHasChanged(currentOffset: Offset, newOffset: Long) = when(currentOffset) {
            is LastProcessedOffset -> newOffset > currentOffset.value
            else -> true
        }

        private fun hasBacklog(page: EventSourcePage): Boolean {
            return page.pageSize > 0 && page.events.isNotEmpty() && (page.pageEndOffset < page.queryMaxOffset)
        }
    }
}

sealed class HttpEventSourceSubscriptionEdenPolicy {

    val objectMapper = ObjectMapper()

    companion object {
        fun from(policy: BoundedContextSubscriptionEdenPolicy) = when(policy) {
            BoundedContextSubscriptionEdenPolicy.FROM_NOW -> FromNow
            BoundedContextSubscriptionEdenPolicy.BEGINNING_OF_TIME -> BeginningOfTime
        }
    }

    abstract fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory

    interface RequestFactory {
        fun createRequest(lastProcessedOffset: Long?): (HttpClient) -> Mono<ObjectNode>
    }
}

object BeginningOfTime : HttpEventSourceSubscriptionEdenPolicy() {
    override fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory {

        return object : RequestFactory {
            override fun createRequest(lastProcessedOffset: Long?): (HttpClient) -> Mono<ObjectNode> {
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
                    client.get().uri(url.toString())
                        .responseContent()
                        .aggregate()
                        .asString()
                        .map { objectMapper.readTree(it).obj }
                        .switchIfEmpty(Mono.error(RuntimeException("Error fetching events")))
                }
            }
        }
    }
}

object FromNow : HttpEventSourceSubscriptionEdenPolicy() {
    override fun newRequestFactory(
            subscriberConfiguration: BoundedContextHttpEventSourceConfiguration,
            tags: Set<DomainEventTag>,
            batchSize: Int): RequestFactory {

        val now = Instant.now() // cache now() once so doesn't refresh on every request

        return object : RequestFactory {
            override fun createRequest(lastProcessedOffset: Long?): (HttpClient) -> Mono<ObjectNode> {
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
                        client.get().uri(url.toString())
                            .responseContent()
                            .aggregate()
                            .asString()
                            .map { objectMapper.readTree(it).obj }
                            .switchIfEmpty(Mono.error(RuntimeException("Error fetching events")))
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
                        client.get().uri(url.toString())
                            .responseContent()
                            .aggregate()
                            .asString()
                            .map { objectMapper.readTree(it).obj }
                            .switchIfEmpty(Mono.error(RuntimeException("Error fetching events")))
                    }
                }
            }
        }
    }
}