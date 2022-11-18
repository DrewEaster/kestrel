package com.dreweaster.ddd.kestrel.infrastructure.rdbms.r2dbc

import com.dreweaster.ddd.kestrel.infrastructure.rdbms.*
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.r2dbc.client.Handle
import io.r2dbc.client.R2dbc
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.*
import java.util.*
import kotlin.text.Regex.Companion.escapeReplacement

class R2dbcDatabase(val r2dbc: R2dbc): Database {

    override fun <T> inTransaction(f: (DatabaseContext) -> Flux<T>): Flux<T> = r2dbc.inTransaction { handle ->
        val queryTransaction = R2dbcDatabaseHandle(handle)
        f(queryTransaction)
    }

    override fun <T> withContext(f: (DatabaseContext) -> Flux<T>): Flux<T> = r2dbc.withHandle { handle ->
        val queryTransaction = R2dbcDatabaseHandle(handle)
        f(queryTransaction)
    }
}

private val objectMapper = ObjectMapper()

class R2dbcResultColumn(columnName: String, row: Row): ResultColumn {

    override val string: String by lazy { checkNotNull(row.get(columnName, java.lang.String::class.java)).toString() }
    override val bool: Boolean by lazy { checkNotNull(row.get(columnName, java.lang.Boolean::class.java)).booleanValue() }
    override val int: Int by lazy { checkNotNull(row.get(columnName, java.lang.Integer::class.java)).toInt() }
    override val long: Long by lazy { checkNotNull(row.get(columnName, java.lang.Long::class.java)).toLong() }
    override val double: Double by lazy { checkNotNull(row.get(columnName, java.lang.Double::class.java)).toDouble() }
    override val float: Float by lazy { checkNotNull(row.get(columnName, java.lang.Float::class.java)).toFloat() }
    override val bigDecimal: BigDecimal by lazy { checkNotNull(row.get(columnName, BigDecimal::class.java)) }
    override val localDate: LocalDate by lazy { checkNotNull(row.get(columnName, LocalDate::class.java)) }
    override val localTime: LocalTime by lazy { checkNotNull(row.get(columnName, LocalTime::class.java)) }
    override val localDateTime: LocalDateTime by lazy { checkNotNull(row.get(columnName, LocalDateTime::class.java)) }
    override val zonedDateTime: ZonedDateTime by lazy { checkNotNull(row.get(columnName, ZonedDateTime::class.java)) }
    override val instant: Instant by lazy { zonedDateTime.toInstant() }
    override val uuid: UUID by lazy { checkNotNull(row.get(columnName, UUID::class.java)) }
    override val stringArray: Array<String> by lazy { row.get(columnName, Array<String>::class.java) ?: emptyArray() }
    override val stringOrNull: String? by lazy { row.get(columnName, java.lang.String::class.java)?.toString() }
    override val intOrNull: Int? by lazy { row.get(columnName, java.lang.Integer::class.java)?.toInt() }
    override val longOrNull: Long? by lazy { row.get(columnName, java.lang.Long::class.java)?.toLong() }
    override val doubleOrNull: Double? by lazy { row.get(columnName, java.lang.Double::class.java)?.toDouble() }
    override val floatOrNull: Float? by lazy { row.get(columnName, java.lang.Float::class.java)?.toFloat() }
    override val boolOrNull: Boolean? by lazy { row.get(columnName, java.lang.Boolean::class.java)?.booleanValue() }
    override val bigDecimalOrNull: BigDecimal? by lazy { row.get(columnName, BigDecimal::class.java) ?: null }
    override val localDateOrNull: LocalDate? by lazy { row.get(columnName, LocalDate::class.java) ?: null }
    override val localTimeOrNull: LocalTime? by lazy { row.get(columnName, LocalTime::class.java) ?: null }
    override val localDateTimeOrNull: LocalDateTime? by lazy { row.get(columnName, LocalDateTime::class.java) ?: null }
    override val zonedDateTimeOrNull: ZonedDateTime? by lazy { row.get(columnName, ZonedDateTime::class.java) ?: null }
    override val instantOrNull: Instant? by lazy { zonedDateTimeOrNull?.toInstant() }
    override val uuidOrNull: UUID? by lazy { row.get(columnName, UUID::class.java) ?: null }
    override val json: JsonNode by lazy { objectMapper.readTree(checkNotNull(row.get(columnName, Json::class.java)).asString()) }
    override val jsonOrNull: JsonNode? by lazy { row.get(columnName, Json::class.java)?.let { objectMapper.readTree(it.asString()) } }
}

class R2dbcResultRow(private val row: Row): ResultRow {
    override fun get(columnName: String) = R2dbcResultColumn(columnName, row)
}

class R2dbcDatabaseHandle(private val handle: Handle): DatabaseContext {

    private val paramValueMapper: (Any) -> Any = { when(it) {
        is Instant -> ZonedDateTime.ofInstant(it, ZoneOffset.UTC)
        else -> it
    }}

    override fun <T> select(sql: String, mapper: (ResultRow) -> T, body: SelectParameterBuilder.() -> Unit): Flux<T> {
        val parameterBuilder = SelectParameterBuilder()
        body(parameterBuilder)
        return select(sql, parameterBuilder.values, mapper)
    }

    override fun <T> select(sql: String, vararg params: Pair<String, Any>, mapper: (ResultRow) -> T): Flux<T> {
        return select(sql, params.toMap(), mapper)
    }

    override fun <T> select(sql: String, mapper: (ResultRow) -> T): Flux<T> {
        return select(sql, emptyMap(), mapper)
    }

    override fun <T> select(sql: String, params: Map<String, Any>, mapper: (ResultRow) -> T): Flux<T> {
        return when {
            params.isEmpty() -> handle.select(sql).mapRow { row -> mapper(R2dbcResultRow(row)) }
            else -> {
                val (translatedSql, translatedParameters) = transformSql(sql, params)
                val query = handle.createQuery(translatedSql)
                translatedParameters.forEach { (column, value) -> when(value) {
                    is NullValue -> query.bindNull(column, value.type.java)
                    else -> query.bind(column, value)
                }}
                query.add().mapResult { result ->
                    val mapped = result.map { row, _ -> mapper(R2dbcResultRow(row)) }
                    Flux.from(mapped).switchIfEmpty(Flux.empty())
                }
            }
        }
    }

    // TODO: does not support list parameter expansion
    override fun <T> batchUpdate(sql: String, values: Iterable<T>, body: UpdateParameterBuilder.(T) -> Unit): Flux<Int> {
        val (transformedSql, parameterIndexMap) = transformSql(sql)
        val update = handle.createUpdate(transformedSql)
        values.forEach { value ->
            val parameterBuilder = UpdateParameterBuilder()
            body(parameterBuilder, value)
            parameterBuilder.values.forEach { (column, value) -> when(value) {
                is NullValue -> parameterIndexMap[column]?.forEach { index -> update.bindNull("$$index", value.type.java) }
                else -> {
                    val paramValue = paramValueMapper(value)
                    parameterIndexMap[column]?.forEach { index -> update.bind("$$index", paramValue) }
                }
            }}
            update.add()
        }
        return update.execute()
    }

    override fun update(sql: String, body: UpdateParameterBuilder.() -> Unit): Flux<Int> {
        val parameterBuilder = UpdateParameterBuilder()
        body(parameterBuilder)
        return update(sql, parameterBuilder.values)
    }

    override fun update(sql: String, vararg params: Pair<String, Any>): Flux<Int> {
        return update(sql, params.toMap())
    }

    override fun update(sql: String, params: Map<String, Any>): Flux<Int> {
        val (translatedSql, translatedParameters) = transformSql(sql, params)
        val update = handle.createUpdate(translatedSql)
        translatedParameters.forEach { (column, value) -> when(value) {
            is NullValue -> update.bindNull(column, value.type.java)
            is JsonNode -> update.bind(column, Json.of(objectMapper.writeValueAsString(value)))
            else -> update.bind(column, value)
        }}
        return update.execute()
    }

    private val extractParametersFromSqlString = Regex(":(.\\w*)")

    private fun transformSql(sql: String): Pair<String, Map<String, List<Int>>> {
        val normalisedSql = removeLineBreaksAndWhitespace(sql)
        val sqlParameterNames = extractParametersFromSqlString.findAll(normalisedSql).map { match -> match.groups[1]?.value }.filter { it != null }.map { it!! }.toList()
        return sqlParameterNames.foldIndexed(normalisedSql to emptyMap()) { index, (transformedSql, indexMap), paramName ->
            val paramIndexes = (indexMap[paramName] ?: emptyList()) + (index + 1)
            val transformedParameterName = "$${index + 1}"
            val replaceRegEx = Regex("(:\\b$paramName\\b)")
            val newSql = transformedSql.replaceFirst(replaceRegEx, escapeReplacement(transformedParameterName))
            newSql to (indexMap + (paramName to paramIndexes))
        }
    }

    private fun transformSql(sql: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
        val (expandedSql, expandedParams) = expandInClauses(removeLineBreaksAndWhitespace(sql), params)
        val sqlParameterNames = extractParametersFromSqlString.findAll(expandedSql).map { match -> match.groups[1]?.value }.filter { it != null }.map { it!! }.toList()
        return sqlParameterNames.foldIndexed(expandedSql to emptyMap()) { index, (transformedSql, transformedParams), name: String ->
            val transformedParameterName = "$${index + 1}"
            val parameterValue = expandedParams[name]
            val replaceRegEx = Regex("(:\\b$name\\b)")
            val newSql = transformedSql.replaceFirst(replaceRegEx, escapeReplacement(transformedParameterName))
            newSql to (transformedParams + (transformedParameterName to paramValueMapper(parameterValue!!)))
        }
    }

    private val extractParametersWithinInClauses = Regex("IN\\s*?\\(\\s*?:(.\\w*)\\s*?\\)")

    private fun expandInClauses(sql: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
        val expandableParameterNames = extractParametersWithinInClauses.findAll(sql).map { match -> match.groups[1]?.value }.filter { it != null }.map { it!! }.toList()
        val (expandedSql, inClauseParams) = expandableParameterNames.fold(sql to emptyMap<String, Any>()) { (sql, expandedParams), name: String ->
            val parameterValue = params[name] as Iterable<*> // TODO: will error if not defined or not a list
            val expandedParameterNames = parameterValue.mapIndexed { index, _ -> ":${name}_$index"}.joinToString(", ")
            val replaceRegEx = Regex("IN\\s*?\\(\\s*?:$name\\s*?\\)")
            sql.replace(replaceRegEx, "IN ($expandedParameterNames)") to expandedParams + (parameterValue.mapIndexed { index, v -> "${name}_$index" to v!!}).toMap()
        }
        return expandedSql to params - expandableParameterNames + inClauseParams
    }

    private fun removeLineBreaksAndWhitespace(sql: String) = sql.replace(Regex("[\\r\\n]+"), " ").replace(Regex("[ ]{2,}")," ")
}

//fun main(args: Array<String>) {
//
//    val extractParametersFromSqlString = Regex(":(.\\w*)")
//    val extractParametersWithinInClauses = Regex("IN\\s*?\\(\\s*?:(.\\w*)\\s*?\\)")
//
//    fun expandInClauses(sql: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
//        val expandableParameterNames = extractParametersWithinInClauses.findAll(sql).map { match -> match.groups[1]?.value }.filter { it != null }.map { it!! }.toList()
//        val (expandedSql, inClauseParams) = expandableParameterNames.fold(sql to emptyMap<String, Any>()) { (sql, expandedParams), name: String ->
//            val parameterValue = params[name] as Iterable<*> // TODO: will error if not defined or not a list
//            val expandedParameterNames = parameterValue.mapIndexed { index, _ -> ":${name}_$index"}.joinToString(", ")
//            val replaceRegEx = Regex("IN\\s*?\\(\\s*?:$name\\s*?\\)")
//            sql.replace(replaceRegEx, "IN ($expandedParameterNames)") to expandedParams + (parameterValue.mapIndexed { index, v -> "${name}_$index" to v!!}).toMap()
//        }
//        return expandedSql to params - expandableParameterNames + inClauseParams
//    }
//
//    fun transformQuery(sql: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
//        val sqlParameterNames = extractParametersFromSqlString.findAll(sql).map { match -> match.groups[1]?.value }.filter { it != null }.map { it!! }.toList()
//        return sqlParameterNames.foldIndexed(sql to emptyMap()) { index, (transformedSql, transformedParams), name: String ->
//            val transformedParameterName = "$${index + 1}"
//            val parameterValue = params[name]
//            val replaceRegEx = Regex("(:\\b$name\\b)")
//            val newSql = transformedSql.replace(replaceRegEx, escapeReplacement(transformedParameterName))
//            newSql to (transformedParams + (transformedParameterName to parameterValue!!))
//        }
//    }
//
//    val params = mapOf(
//        "values" to listOf(1,2,3),
//        "types" to listOf("atype", "btype", "ctype", "dtype", "etype"),
//        "date" to Instant.now(),
//        "age" to 18
//    )
//
//    val sql = "SELECT * from user where id IN (:values) AND date > :date AND age = :age AND type IN (:types)"
//    val (expandedSql, expandedParams) = expandInClauses(sql, params)
//    println(expandedSql)
//    println(expandedParams)
//
//    val (transformedSql, transformedParams) = transformQuery(expandedSql, expandedParams)
//    println(transformedSql)
//    println(transformedParams)
//}

fun main(args: Array<String>) {
     val sql =
        """
            SELECT global_offset, event_id, aggregate_id, aggregate_type, causation_id, correlation_id, event_type, event_version, event_payload, event_timestamp, sequence_number
            FROM domain_event
            WHERE tag IN (:tags) AND global_offset > :after_offset
            ORDER BY global_offset
            LIMIT :limit
        """

    println(sql.replace(Regex("[\\r\\n]+"), " ").replace(Regex("[ ]{2,}")," "))
}