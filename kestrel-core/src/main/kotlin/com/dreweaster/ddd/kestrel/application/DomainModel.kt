package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import java.util.*

object IdGenerator {
    fun randomId(): String = UUID.randomUUID().toString().replace("-","")
}

data class AggregateId(val value: String = IdGenerator.randomId())
data class CommandId(val value: String = IdGenerator.randomId())
data class CausationId(val value: String = IdGenerator.randomId())
data class CorrelationId(val value: String = IdGenerator.randomId())
data class EventId(val value: String = IdGenerator.randomId())

sealed class CommandHandlingResult<E: DomainEvent>
data class SuccessResult<E: DomainEvent>(val generatedEvents: List<E>, val deduplicated: Boolean = false) : CommandHandlingResult<E>()
data class RejectionResult<E: DomainEvent>(val error: Throwable, val deduplicated: Boolean = false) : CommandHandlingResult<E>()
class ConcurrentModificationResult<E: DomainEvent> : CommandHandlingResult<E>()
class UnexpectedExceptionResult<E: DomainEvent>(val ex: Throwable): CommandHandlingResult<E>()

// General errors
object UnsupportedCommandInEdenBehaviour : RuntimeException()
object UnsupportedCommandInCurrentBehaviour : RuntimeException()
object AggregateInstanceAlreadyExists : RuntimeException()
object UnsupportedEventInEdenBehaviour: RuntimeException()
object UnsupportedEventInCurrentBehaviour: RuntimeException()

data class CommandEnvelope<C: DomainCommand>(
        val command: C,
        val commandId: CommandId = CommandId(UUID.randomUUID().toString().replace("-","")),
        val causationId: CausationId? = null,
        val correlationId: CorrelationId? = null
)

interface AggregateRoot<C: DomainCommand, E: DomainEvent> {

    suspend infix fun handle(commandEnvelope: CommandEnvelope<C>): CommandHandlingResult<E>

    suspend infix fun handle(command: C): CommandHandlingResult<E> = handle(CommandEnvelope(command))
}

interface DomainModel {

    fun <C: DomainCommand, E: DomainEvent, S: AggregateState> aggregateRootOf(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId = AggregateId()): AggregateRoot<C, E>

    fun addReporter(reporter: DomainModelReporter)

    fun removeReporter(reporter: DomainModelReporter)
}