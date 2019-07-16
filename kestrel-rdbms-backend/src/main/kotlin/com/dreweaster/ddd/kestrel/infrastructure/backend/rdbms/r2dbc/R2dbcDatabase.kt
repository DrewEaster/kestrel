package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.r2dbc

import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.DatabaseTransaction
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultColumn
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultRow
import io.r2dbc.client.Handle
import io.r2dbc.client.R2dbc
import io.r2dbc.spi.Row
import reactor.core.publisher.Flux
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

//    var configuration = PostgresqlConnectionConfiguration.builder()
//        .host("<host>")
//        .database("<database>")
//        .username("<username>")
//        .password("<password>")
//        .build()
//
//    var r2dbc = R2dbc(PostgresqlConnectionFactory(configuration))

class R2dbcDatabase(val r2dbc: R2dbc): Database {

    override fun <T> inTransaction(f: (DatabaseTransaction) -> Flux<out T>): Flux<T> = r2dbc.inTransaction { handle ->
        val queryTransaction = R2dbcTransaction(handle)
        f(queryTransaction)
    }
}

class R2dbcResultColumn(columnName: String, row: Row): ResultColumn {
    override val string: String by lazy { row.get(columnName, String::class.java) }
    override val bool: Boolean by lazy { row.get(columnName, Boolean::class.java) }
    override val instant: Instant by lazy { zonedDateTime.toInstant() }
}

class R2dbcResultRow(private val row: Row): ResultRow {
    override fun get(columnName: String) = R2dbcResultColumn(columnName, row)
}

class R2dbcTransaction(private val handle: Handle): DatabaseTransaction {

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

