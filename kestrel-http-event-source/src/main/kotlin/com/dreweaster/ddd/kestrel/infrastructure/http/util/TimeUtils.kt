package com.dreweaster.ddd.kestrel.infrastructure.http.util

import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

object TimeUtils {

    private val ZONE_ID_UTC = TimeZone.getTimeZone("UTC").toZoneId()

    private val DATE_FORMAT = DateTimeFormatter.ISO_INSTANT.withZone(ZONE_ID_UTC)

    fun instantToUTCString(instant: Instant): String {
        return instant.atZone(ZONE_ID_UTC).format(DATE_FORMAT)
    }

    fun instantFromUTCString(dateTime: String): Instant {
        return ZonedDateTime.from(DATE_FORMAT.parse(dateTime)).toInstant()
    }
}