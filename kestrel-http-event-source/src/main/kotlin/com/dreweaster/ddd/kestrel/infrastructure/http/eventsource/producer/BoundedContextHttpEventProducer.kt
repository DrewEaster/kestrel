package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.producer

import com.dreweaster.ddd.kestrel.application.Backend
import com.dreweaster.ddd.kestrel.application.EventFeed
import com.dreweaster.ddd.kestrel.application.FeedEvent
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.producer.EventPayloadSerialisationStrategy.Companion.default
import com.dreweaster.ddd.kestrel.infrastructure.http.util.TimeUtils
import com.dreweaster.ddd.kestrel.util.json.jsonArray
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import reactor.core.publisher.Mono

class BoundedContextHttpJsonEventProducer(val backend: Backend) {

    fun produceFrom(
        urlQueryParameters: Map<String, List<String>>,
        serialisationStrategy: EventPayloadSerialisationStrategy = default): Mono<ObjectNode> {
        return convertStreamToJsonResponse(fetchEvents(HttpJsonEventQuery.from(urlQueryParameters)), serialisationStrategy)
    }

    private fun fetchEvents(query: HttpJsonEventQuery): Mono<EventFeed> {
        return if(query.afterTimestamp != null) {
            backend.fetchEventFeed<DomainEvent>(
                query.tags,
                query.afterTimestamp,
                query.batchSize)
        } else {
            backend.fetchEventFeed<DomainEvent>(
                query.tags,
                query.afterOffset ?: -1L,
                query.batchSize)
        }
    }

    private fun convertStreamToJsonResponse(monoFeed: Mono<EventFeed>, serialisationStrategy: EventPayloadSerialisationStrategy) =
        monoFeed.map { feed ->
            jsonObject(
                "tags" to jsonArray(feed.tags.map { it.value }),
                "batch_size" to feed.pageSize,
                "page_start_offset" to feed.pageStartOffset,
                "page_end_offset" to feed.pageEndOffset,
                "query_max_offset" to feed.queryMaxOffset,
                "global_max_offset" to feed.globalMaxOffset,
                "events" to jsonArray(feed.events.map { streamEventToJsonEvent(it, serialisationStrategy) })
        )}

    private fun streamEventToJsonEvent(event: FeedEvent, serialisationStrategy: EventPayloadSerialisationStrategy) =
        jsonObject(
            "offset" to event.offset,
            "id" to event.id.value,
            "aggregate_type" to event.aggregateType,
            "aggregate_id" to event.aggregateId.value,
            "causation_id" to event.causationId.value,
            "correlation_id" to event.correlationId?.value,
            "type" to event.eventType,
            "tag" to event.eventTag.value,
            "timestamp" to TimeUtils.instantToUTCString(event.timestamp),
            "sequence_number" to event.sequenceNumber,
            "payload" to serialisationStrategy.serialise(event.serialisedPayload),
            "version" to event.eventVersion
        )
}

interface EventPayloadSerialisationStrategy {

    companion object {
        val default = object : EventPayloadSerialisationStrategy {
            override fun serialise(value: String): JsonNode = TextNode(value)
        }

        fun json(objectMapper: ObjectMapper): EventPayloadSerialisationStrategy {
            return object : EventPayloadSerialisationStrategy {
                override fun serialise(value: String): JsonNode = objectMapper.readTree(value)
            }
        }
    }

    fun serialise(value: String): JsonNode
}