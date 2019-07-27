package com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.producer

import com.dreweaster.ddd.kestrel.application.Backend
import com.dreweaster.ddd.kestrel.application.EventStream
import com.dreweaster.ddd.kestrel.application.StreamEvent
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.HttpJsonEventQuery
import com.dreweaster.ddd.kestrel.infrastructure.http.util.TimeUtils
import com.github.salomonbrys.kotson.jsonArray
import com.github.salomonbrys.kotson.jsonObject
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import reactor.core.publisher.Mono

class BoundedContextHttpJsonEventStreamProducer(val backend: Backend) {

    private val jsonParser = JsonParser()

    fun produceFrom(urlQueryParameters: Map<String, List<String>>): Mono<JsonObject> {
        return convertStreamToJsonResponse(fetchEventStream(HttpJsonEventQuery.from(urlQueryParameters ?: emptyMap())))
    }

    private fun fetchEventStream(query: HttpJsonEventQuery): Mono<EventStream> {
        return if(query.afterTimestamp != null) {
            backend.loadEventStream<DomainEvent>(
                query.tags,
                query.afterTimestamp,
                query.batchSize)
        } else {
            backend.loadEventStream<DomainEvent>(
                query.tags,
                query.afterOffset ?: -1L,
                query.batchSize)
        }
    }

    private fun convertStreamToJsonResponse(singleStream: Mono<EventStream>) =
        singleStream.map { stream ->
            jsonObject(
                "tags" to jsonArray(stream.tags.map { it.value }),
                "batch_size" to stream.batchSize,
                "start_offset" to stream.startOffset,
                "end_offset" to stream.endOffset,
                "max_offset" to stream.maxOffset,
                "events" to jsonArray(stream.events.map { streamEventToJsonEvent(it) })
        )}

    private fun streamEventToJsonEvent(event: StreamEvent) =
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