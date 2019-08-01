package com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import kotlin.reflect.KClass

data class ProjectionStatement(val sql: String, val parameters: Map<String, Any>, val expectedRowsAffected: Int? = null): Iterable<ProjectionStatement> {
    override fun iterator(): Iterator<ProjectionStatement> = listOf(this).iterator()
}

abstract class ConsistentDatabaseProjection {

    class EventHandlers {

        var handlers: Map<KClass<out DomainEvent>, ((PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>)> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>): EventHandlers {
            handlers += type to handler
            return this
        }
    }

    abstract val update: Projection<*, *>

    inline fun <reified A: Aggregate<*,E,*>, E: DomainEvent> projection(init: Projection<E, A>.() -> Unit): Projection<E, A> {
        val projection = Projection(A::class)
        projection.init()
        return projection
    }
}

// TODO: Restrict events to only those applicable to the aggregate type
class Projection<E: DomainEvent, A: Aggregate<*, E, *>>(val aggregateType: KClass<A>) {

    val eventHandlers = ConsistentDatabaseProjection.EventHandlers()

    inline fun <reified Evt: E> event(noinline handler: (PersistedEvent<Evt>) -> Iterable<ProjectionStatement>) {
        eventHandlers.withHandler(Evt::class, handler as (PersistedEvent<DomainEvent>) -> Iterable<ProjectionStatement>)
    }

    fun <Evt: DomainEvent> getProjectionStatements(e: PersistedEvent<Evt>): Iterable<ProjectionStatement> {
        return eventHandlers.handlers[e.rawEvent::class]?.invoke(e as PersistedEvent<DomainEvent>) ?: emptyList()
    }

    fun statement(sql: String, body: UpdateParameterBuilder.() -> Unit): ProjectionStatement {
        val parameterBuilder = UpdateParameterBuilder()
        body(parameterBuilder)
        return ProjectionStatement(sql, parameterBuilder.values)
    }

    fun String.params(vararg params: Pair<String, Any>) = ProjectionStatement(sql = this, parameters = params.toMap())

    fun ProjectionStatement.expect(expectedRowsAffected: Int) = this.copy(expectedRowsAffected = expectedRowsAffected)
}

