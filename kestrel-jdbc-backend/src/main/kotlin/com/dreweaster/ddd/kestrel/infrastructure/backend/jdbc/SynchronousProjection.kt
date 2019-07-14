package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import org.jetbrains.exposed.sql.transactions.TransactionManager
import kotlin.reflect.KClass

abstract class SynchronousProjection {

    class EventHandlers {

        var handlers: Map<KClass<out DomainEvent>, ((PersistedEvent<DomainEvent>) -> Unit)> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (PersistedEvent<DomainEvent>) -> Unit): EventHandlers {
            handlers += type to handler
            return this
        }
    }

    abstract val projection: ProjectionBuilder<*,*>

    inline fun <reified A: Aggregate<*,E,*>, E: DomainEvent> projectFrom(init: ProjectionBuilder<E, A>.() -> Unit): ProjectionBuilder<E, A> {
        val projection = ProjectionBuilder(A::class)
        projection.init()
        return projection
    }

    infix fun Int.eq(number: Int) {
        if(this != number) {
            throw UnexpectedNumberOfRowsAffectedInUpdate(number, this)
        }
    }
}



data class ProjectionQuery(val parameterisedSql: String, val parameters: List<Any>, val expectedRowsAffected: Int)

// TODO: Restrict events to only those applicable to the aggregate type
class ProjectionBuilder<E: DomainEvent, A: Aggregate<*, E, *>>(val aggregateType: KClass<A>) {

    val eventHandlers = SynchronousJdbcReadModel.EventHandlers()

    inline fun <reified Evt: E> event(noinline handler: (PersistedEvent<Evt>) -> Unit) {
        eventHandlers.withHandler(Evt::class, handler as (PersistedEvent<DomainEvent>) -> Unit)
    }

    fun handleEvent(e: PersistedEvent<DomainEvent>) {
        eventHandlers.handlers[e.rawEvent::class]?.invoke(e)
    }
}