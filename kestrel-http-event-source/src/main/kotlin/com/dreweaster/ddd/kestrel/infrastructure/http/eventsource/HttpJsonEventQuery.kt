package com.dreweaster.ddd.kestrel.infrastructure.http.eventsource

import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.infrastructure.http.util.TimeUtils.instantFromUTCString
import com.dreweaster.ddd.kestrel.infrastructure.http.util.TimeUtils.instantToUTCString
import java.net.URL
import java.net.URLEncoder
import java.time.Instant

data class HttpJsonEventQuery(
        val tags: Set<DomainEventTag>? = null,
        val afterOffset: Long? = null,
        val afterTimestamp: Instant? = null,
        val batchSize: Int) {

    companion object {
        fun from(urlQueryParameters: Map<String, List<String>>): HttpJsonEventQuery {
            val tags = urlQueryParameters["tags"]?.first()?.split(",")?.map { DomainEventTag(it) }
            val afterTimestamp = urlQueryParameters["after_timestamp"]?.firstOrNull()?.let { instantFromUTCString(it) }
            val afterOffset = urlQueryParameters["after_offset"]?.firstOrNull()?.toLong()
            val batchSize = urlQueryParameters["batch_size"]?.firstOrNull()?.toInt() ?: 10
            return HttpJsonEventQuery(tags?.toSet(), afterOffset, afterTimestamp, batchSize)
        }
    }

    fun eventsUrlFor(protocol: String, hostname: String, port: Int, path: String) = when {
        afterTimestamp != null -> URL("$protocol://$hostname:$port$path?${tagsQueryParam(tags)}batch_size=$batchSize&after_timestamp=${URLEncoder.encode(instantToUTCString(afterTimestamp), "UTF-8")}")
        else -> URL("$protocol://$hostname:$port$path?${tagsQueryParam(tags)}batch_size=$batchSize&after_offset=${afterOffset ?: -1}")
    }

    private fun tagsQueryParam(tags: Set<DomainEventTag>?): String = tags?.let {"tags=${it.joinToString(",") { tag -> tag.value }}&"} ?: ""
}