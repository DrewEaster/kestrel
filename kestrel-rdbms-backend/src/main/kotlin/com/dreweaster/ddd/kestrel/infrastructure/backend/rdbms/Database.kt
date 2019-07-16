package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms

import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.*
import java.util.*

interface ResultColumn {
    val string: String
    val int: Int
    val long: Long
    val double: Double
    val float: Float
    val bool: Boolean
    val bigDecimal: BigDecimal
    val localDate: LocalDate
    val localTime: LocalTime
    val localDateTime: LocalDateTime
    val zonedDateTime: ZonedDateTime
    val instant: Instant
    val uuid: UUID

    val stringOrNull: String?
    val intOrNull: Int?
    val longOrNull: Long?
    val doubleOrNull: Double?
    val floatOrNull: Float?
    val boolOrNull: Boolean?
    val bigDecimalOrNull: BigDecimal?
    val localDateOrNull: LocalDate?
    val localTimeOrNull: LocalTime?
    val localDateTimeOrNull: LocalDateTime?
    val zonedDateTimeOrNull: ZonedDateTime?
    val instantOrNull: Instant?
    val uuidOrNull: UUID?
}

interface ResultRow {
    operator fun get(columnName: String): ResultColumn
}

interface DatabaseTransaction {
    fun <T> select(sql: String, vararg params: Pair<String, Any>, mapper: (ResultRow) -> T): Flux<out T>
    fun <T> select(sql: String, mapper: (ResultRow) -> T): Flux<out T>
}

interface Database {
    fun <T> inTransaction(f: (DatabaseTransaction) -> Flux<out T>): Flux<T>
}