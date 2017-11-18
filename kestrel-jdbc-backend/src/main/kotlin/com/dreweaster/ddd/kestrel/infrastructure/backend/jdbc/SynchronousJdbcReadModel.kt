package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.github.andrewoma.kwery.core.Session

interface SynchronousJdbcReadModel {

    fun aggregateType(): Aggregate<*, *, *>

    fun <E: DomainEvent> update(event: PersistedEvent<E>, session: Session, tx: Transaction)

    fun execute(f: (Session, Transaction) -> Unit, session: Session, tx: Transaction) = f(session, tx)
}