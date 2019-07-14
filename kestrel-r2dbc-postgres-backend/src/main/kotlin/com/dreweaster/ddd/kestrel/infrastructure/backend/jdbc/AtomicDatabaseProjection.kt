package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.r2dbc.client.Handle
import io.r2dbc.client.R2dbc
import io.r2dbc.spi.Row
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.function.Function
import kotlin.reflect.KClass

data class ProjectionStatement(val sql: String, val parameters: List<Any>, val expectedRowsAffected: Int? = null): Iterable<ProjectionStatement> {
    override fun iterator(): Iterator<ProjectionStatement> = listOf(this).iterator()
}

abstract class AtomicDatabaseProjection {

//    var configuration = PostgresqlConnectionConfiguration.builder()
//            .host("<host>")
//            .database("<database>")
//            .username("<username>")
//            .password("<password>")
//            .build()
//
//    var r2dbc = R2dbc(PostgresqlConnectionFactory(configuration))
//
//    init {
//        r2dbc.inTransaction { handle -> handle. }
//    }

    class EventHandlers {

        var handlers: Map<KClass<out DomainEvent>, ((PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>)> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>): EventHandlers {
            handlers += type to handler
            return this
        }
    }

    abstract val update: Projection<*,*>

    inline fun <reified A: Aggregate<*,E,*>, E: DomainEvent> projection(init: Projection<E, A>.() -> Unit): Projection<E, A> {
        val projection = Projection(A::class)
        projection.init()
        return projection
    }

    infix fun Int.eq(number: Int) {
        if(this != number) {
            throw UnexpectedNumberOfRowsAffectedInUpdate(number, this)
        }
    }
}

// TODO: Restrict events to only those applicable to the aggregate type
class Projection<E: DomainEvent, A: Aggregate<*, E, *>>(val aggregateType: KClass<A>) {

    val eventHandlers = AtomicDatabaseProjection.EventHandlers()

    inline fun <reified Evt: E> event(noinline handler: (PersistedEvent<Evt>) -> Iterable<ProjectionStatement>) {
        eventHandlers.withHandler(Evt::class, handler as (PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>)
    }

    fun handleEvent(e: PersistedEvent<DomainEvent>) {
        eventHandlers.handlers[e.rawEvent::class]?.invoke(e)
    }

    fun String.params(vararg params: Any) = ProjectionStatement(sql = this, parameters = params.toList())

    fun ProjectionStatement.expect(expectedRowsAffected: Int) = this.copy(expectedRowsAffected = expectedRowsAffected)
}

data class Query<T>(val sql: String, val parameters: List<Any>, val rowMapper: Function<Row, out T>? = null)


class QueryTransaction(private val handle: Handle) {
    fun <T> query(sql: String, vararg params: String, mapper: (Row) -> T): Publisher<out T> {
        return handle.select(sql, params).mapRow(mapper)
    }

    fun <T> query(sql: String, mapper: (Row) -> T): Publisher<out T> {
        return handle.select(sql).mapRow(mapper)
    }
}

class DB(private val r2dbc: R2dbc) {

    fun <T> inTransaction(f: (QueryTransaction) -> Publisher<out T>): Flux<T> = r2dbc.inTransaction { handle ->
        val queryTransaction = QueryTransaction(handle)
        f(queryTransaction)
    }
}

interface DatabaseReadModel {
    fun String.params(vararg params: Any) = Query(sql = this, parameters = params.toList())
    fun <T> Query<T>.map(mapper: Function<Row, out T>) = this.copy(rowMapper = mapper)
}
