package com.dreweaster.ddd.kestrel.infrastructure.rdbms

import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.*
import java.util.*
import kotlin.reflect.KClass

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
    val stringArray: Array<String>

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

data class SelectParameterBuilder(val values: MutableMap<String, Any> = mutableMapOf()) {

    operator fun set(column: String, value: Any) {
        values[column] = value
    }
}

data class NullValue(val type: KClass<out Any>)

data class UpdateParameterBuilder(val values: MutableMap<String, Any> = mutableMapOf()) {

    inline fun <reified T: Any> nullable(value: T?): Any = when(value) {
        null -> NullValue(T::class)
        else -> value
    }

    inline fun <reified T: Any> nullValue() = NullValue(T::class)

    operator fun set(column: String, value: Any) {
        values[column] = value
    }
}

interface DatabaseContext {
    fun <T> select(sql: String, mapper: (ResultRow) -> T, body: SelectParameterBuilder.() -> Unit): Flux<T>
    fun <T> select(sql: String, vararg params: Pair<String, Any>, mapper: (ResultRow) -> T): Flux<T>
    fun <T> select(sql: String, params: Map<String, Any>, mapper: (ResultRow) -> T): Flux<T>
    fun <T> select(sql: String, mapper: (ResultRow) -> T): Flux<T>
    fun <T> batchUpdate(sql: String, values: Iterable<T>, body: UpdateParameterBuilder.(T) -> Unit): Flux<Int>
    fun update(sql: String, body: UpdateParameterBuilder.() -> Unit): Flux<Int>
    fun update(sql: String, vararg params: Pair<String, Any>): Flux<Int>
    fun update(sql: String, params: Map<String, Any>): Flux<Int>
}

interface Database {
    fun <T> withContext(f: (DatabaseContext) -> Flux<T>): Flux<T>
    fun <T> inTransaction(f: (DatabaseContext) -> Flux<T>): Flux<T>
}