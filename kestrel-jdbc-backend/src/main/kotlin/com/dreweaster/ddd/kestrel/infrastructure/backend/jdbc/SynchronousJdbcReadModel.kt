package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import kotlin.reflect.KClass

abstract class SynchronousJdbcReadModel {

    class EventHandlers {

        var handlers: Map<KClass<out DomainEvent>, ((DatabaseTransaction, PersistedEvent<DomainEvent>) -> Unit)> = emptyMap()

        fun <E: DomainEvent> withHandler(type: KClass<E>, handler: (DatabaseTransaction, PersistedEvent<DomainEvent>) -> Unit): EventHandlers {
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

    infix fun Int.assertEquals(number: Int) {

    }
}

// TODO: Restrict events to only those applicable to the aggregate type
class Projection<E: DomainEvent, A: Aggregate<*, E, *>>(val aggregateType: KClass<A>) {

    val eventHandlers = SynchronousJdbcReadModel.EventHandlers()

    inline fun <reified Evt: E> event(noinline handler: (DatabaseTransaction, PersistedEvent<Evt>) -> Unit) {
        eventHandlers.withHandler(Evt::class, handler as (DatabaseTransaction, PersistedEvent<DomainEvent>) -> Unit)
    }

    fun handleEvent(tx: DatabaseTransaction, e: PersistedEvent<DomainEvent>) {
        eventHandlers.handlers[e.rawEvent::class]?.invoke(tx, e)
    }
}