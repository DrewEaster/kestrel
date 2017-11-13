package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import java.util.*

data class AggregateId(val value: String)
data class CommandId(val value: String)
data class CausationId(val value: String)
data class CorrelationId(val value: String)
data class EventId(val value: String)

sealed class CommandHandlingResult<E: DomainEvent>
data class SuccessResult<E: DomainEvent>(val generatedEvents: List<E>, val deduplicated: Boolean = false) : CommandHandlingResult<E>()
data class RejectionResult<E: DomainEvent>(val error: Throwable, val deduplicated: Boolean = false) : CommandHandlingResult<E>()
class ConcurrentModificationResult<E: DomainEvent> : CommandHandlingResult<E>()
class UnexpectedExceptionResult<E: DomainEvent>(ex: Throwable): CommandHandlingResult<E>()

// General errors
object UnsupportedCommandInEdenBehaviour : RuntimeException()
object UnsupportedCommandInCurrentBehaviour : RuntimeException()
object AggregateInstanceAlreadyExists : RuntimeException()
object UnsupportedEventInEdenBehaviour: RuntimeException()
object UnsupportedEventInCurrentBehaviour: RuntimeException()

data class CommandEnvelope<C: DomainCommand>(
    val commandId: CommandId,
    val command: C,
    val causationId: Optional<CausationId> = Optional.empty(),
    val correlationId: Optional<CorrelationId> = Optional.empty()
)

interface AggregateRoot<C: DomainCommand, E: DomainEvent> {

    suspend fun handle(commandEnvelope: CommandEnvelope<C>): CommandHandlingResult<E>
}

interface DomainModel {

    fun <C: DomainCommand, E: DomainEvent, S: AggregateState> aggregateRootOf(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId): AggregateRoot<C, E>

    fun addReporter(reporter: DomainModelReporter)

    fun removeReporter(reporter: DomainModelReporter)
}