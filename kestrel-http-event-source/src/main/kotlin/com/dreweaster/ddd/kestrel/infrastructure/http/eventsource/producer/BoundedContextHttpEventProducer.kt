package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.producer

import com.dreweaster.ddd.kestrel.application.Backend
import com.dreweaster.ddd.kestrel.application.EventFeed
import com.dreweaster.ddd.kestrel.application.FeedEvent
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.infrastructure.http.util.TimeUtils
import com.github.salomonbrys.kotson.jsonArray
import com.github.salomonbrys.kotson.jsonObject
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import reactor.core.publisher.Mono

class BoundedContextHttpJsonEventProducer(val backend: Backend) {

    private val jsonParser = JsonParser()

    fun produceFrom(urlQueryParameters: Map<String, List<String>>): Mono<JsonObject> {
        return convertStreamToJsonResponse(fetchEvents(HttpJsonEventQuery.from(urlQueryParameters)))
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

    private fun convertStreamToJsonResponse(monoFeed: Mono<EventFeed>) =
        monoFeed.map { feed ->
            jsonObject(
                "tags" to jsonArray(feed.tags.map { it.value }),
                "batch_size" to feed.pageSize,
                "page_start_offset" to feed.pageStartOffset,
                "page_end_offset" to feed.pageEndOffset,
                "query_max_offset" to feed.queryMaxOffset,
                "global_max_offset" to feed.globalMaxOffset,
                "events" to jsonArray(feed.events.map { streamEventToJsonEvent(it) })
        )}

    private fun streamEventToJsonEvent(event: FeedEvent) =
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
            "payload" to jsonParser.parse(event.serialisedPayload)
        )
}