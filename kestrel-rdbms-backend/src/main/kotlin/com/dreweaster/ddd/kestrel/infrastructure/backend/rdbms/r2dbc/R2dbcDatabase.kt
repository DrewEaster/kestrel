package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.r2dbc

import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.DatabaseContext
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultColumn
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultRow
import io.r2dbc.client.Handle
import io.r2dbc.client.R2dbc
import io.r2dbc.spi.Row
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.*
import java.util.*

//    var configuration = PostgresqlConnectionConfiguration.builder()
//        .host("<host>")
//        .database("<database>")
//        .username("<username>")
//        .password("<password>")
//        .build()
//
//    var r2dbc = R2dbc(PostgresqlConnectionFactory(configuration))

class R2dbcDatabase(val r2dbc: R2dbc): Database {

    override fun <T> inTransaction(f: (DatabaseContext) -> Flux<out T>): Flux<T> = r2dbc.inTransaction { handle ->
        val queryTransaction = R2dbcDatabaseHandle(handle)
        f(queryTransaction)
    }
}

class R2dbcResultColumn(columnName: String, row: Row): ResultColumn {

    override val string: String by lazy { checkNotNull(row.get(columnName, String::class.java)) }
    override val bool: Boolean by lazy { checkNotNull(row.get(columnName, Boolean::class.java)) }
    override val int: Int by lazy { checkNotNull(row.get(columnName, Int::class.java)) }
    override val long: Long by lazy { checkNotNull(row.get(columnName, Long::class.java)) }
    override val double: Double by lazy { checkNotNull(row.get(columnName, Double::class.java)) }
    override val float: Float by lazy { checkNotNull(row.get(columnName, Float::class.java)) }
    override val bigDecimal: BigDecimal by lazy { checkNotNull(row.get(columnName, BigDecimal::class.java)) }
    override val localDate: LocalDate by lazy { checkNotNull(row.get(columnName, LocalDate::class.java)) }
    override val localTime: LocalTime by lazy { checkNotNull(row.get(columnName, LocalTime::class.java)) }
    override val localDateTime: LocalDateTime by lazy { checkNotNull(row.get(columnName, LocalDateTime::class.java)) }
    override val zonedDateTime: ZonedDateTime by lazy { checkNotNull(row.get(columnName, ZonedDateTime::class.java)) }
    override val instant: Instant by lazy { zonedDateTime.toInstant() }
    override val uuid: UUID by lazy { checkNotNull(row.get(columnName, UUID::class.java)) }

    override val stringOrNull: String? by lazy { row.get(columnName, String::class.java) ?: null }
    override val intOrNull: Int? by lazy { row.get(columnName, Int::class.java) ?: null }
    override val longOrNull: Long? by lazy { row.get(columnName, Long::class.java) ?: null }
    override val doubleOrNull: Double? by lazy { row.get(columnName, Double::class.java) ?: null }
    override val floatOrNull: Float? by lazy { row.get(columnName, Float::class.java) ?: null }
    override val boolOrNull: Boolean? by lazy { row.get(columnName, Boolean::class.java) ?: null }
    override val bigDecimalOrNull: BigDecimal? by lazy { row.get(columnName, BigDecimal::class.java) ?: null }
    override val localDateOrNull: LocalDate? by lazy { row.get(columnName, LocalDate::class.java) ?: null }
    override val localTimeOrNull: LocalTime? by lazy { row.get(columnName, LocalTime::class.java) ?: null }
    override val localDateTimeOrNull: LocalDateTime? by lazy { row.get(columnName, LocalDateTime::class.java) ?: null }
    override val zonedDateTimeOrNull: ZonedDateTime? by lazy { row.get(columnName, ZonedDateTime::class.java) ?: null }
    override val instantOrNull: Instant? by lazy { zonedDateTimeOrNull?.toInstant() }
    override val uuidOrNull: UUID? by lazy { row.get(columnName, UUID::class.java) ?: null }
}

class R2dbcResultRow(private val row: Row): ResultRow {
    override fun get(columnName: String) = R2dbcResultColumn(columnName, row)
}

class R2dbcDatabaseHandle(private val handle: Handle): DatabaseContext {

    private val paramValueMapper: (Any) -> Any = { when(it) {
        is Instant -> ZonedDateTime.ofInstant(it, ZoneOffset.UTC)
        else -> it
    }}

    override fun <T> select(sql: String, vararg params: Pair<String, Any>, mapper: (ResultRow) -> T): Flux<out T> {
        return handle.select(sql, params.map(paramValueMapper)).mapRow { row -> mapper(R2dbcResultRow(row)) }
    }

    override fun <T> select(sql: String, mapper: (ResultRow) -> T): Flux<out T> {
        return handle.select(sql).mapRow { row -> mapper(R2dbcResultRow(row)) }
    }
}

