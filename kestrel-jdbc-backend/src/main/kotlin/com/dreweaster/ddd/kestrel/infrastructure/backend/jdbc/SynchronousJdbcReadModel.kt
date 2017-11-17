package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.github.andrewoma.kwery.core.Session

interface SynchronousJdbcReadModel {

    val aggregateType: String

    fun <E: DomainEvent> update(events: List<PersistedEvent<E>>, session: Session, tx: Transaction)
}