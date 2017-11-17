package com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.github.andrewoma.kwery.core.Session

interface SynchronousJdbcReadModel {

    val aggregateType: String

    fun <E: DomainEvent> update(
            aggregateType: Aggregate<*,E,*>,
            aggregateId: AggregateId,
            events: List<PersistedEvent<E>>,
            session: Session,
            tx: Transaction)
}