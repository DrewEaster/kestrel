package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import io.reactivex.Single
import java.util.*

object IdGenerator {
    fun randomId(): String = UUID.randomUUID().toString().replace("-","")
}

data class ProcessManagerCorrelationId(val value: String = IdGenerator.randomId())
data class AggregateId(val value: String = IdGenerator.randomId())
data class CommandId(val value: String = IdGenerator.randomId())
data class CausationId(val value: String = IdGenerator.randomId())
data class CorrelationId(val value: String = IdGenerator.randomId())
data class EventId(val value: String = IdGenerator.randomId())

sealed class CommandHandlingResult<C: DomainCommand, E: DomainEvent> {
    abstract val command: CommandEnvelope<C>
}
data class SuccessResult<C: DomainCommand, E: DomainEvent>(override val command: CommandEnvelope<C>, val generatedEvents: List<E>, val deduplicated: Boolean = false) : CommandHandlingResult<C, E>()
data class RejectionResult<C: DomainCommand, E: DomainEvent>(override val command: CommandEnvelope<C>, val error: Throwable, val deduplicated: Boolean = false) : CommandHandlingResult<C, E>()
class ConcurrentModificationResult<C: DomainCommand, E: DomainEvent>(override val command: CommandEnvelope<C>) : CommandHandlingResult<C, E>()
class UnexpectedExceptionResult<C: DomainCommand, E: DomainEvent>(override val command: CommandEnvelope<C>, val ex: Throwable): CommandHandlingResult<C, E>()

// General errors
object UnsupportedCommandInEdenBehaviour : RuntimeException()
object UnsupportedCommandInCurrentBehaviour : RuntimeException()
object AggregateInstanceAlreadyExists : RuntimeException()
object UnsupportedEventInEdenBehaviour: RuntimeException()
object UnsupportedEventInCurrentBehaviour: RuntimeException()

data class CommandEnvelope<C: DomainCommand>(
        val command: C,
        val commandId: CommandId = CommandId(UUID.randomUUID().toString().replace("-", "")),
        val causationId: CausationId? = null,
        val correlationId: CorrelationId? = null
)

interface AggregateRoot<C: DomainCommand, E: DomainEvent> {

    infix fun handleCommandEnvelope(commandEnvelope: CommandEnvelope<C>): Single<CommandHandlingResult<C, E>>

    infix fun handleCommand(command: C): Single<CommandHandlingResult<C, E>> = handleCommandEnvelope(CommandEnvelope(command))
}

interface DomainModel {

    fun <C: DomainCommand, E: DomainEvent, S: AggregateState> aggregateRootOf(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId = AggregateId()): AggregateRoot<C, E>

    fun addReporter(reporter: DomainModelReporter)

    fun removeReporter(reporter: DomainModelReporter)
}