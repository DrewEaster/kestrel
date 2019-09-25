package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import reactor.core.publisher.Mono
import java.util.*

object IdGenerator {
    fun randomId(): String = UUID.randomUUID().toString().replace("-","")
}

data class AggregateId(val value: String = IdGenerator.randomId())
data class CommandId(val value: String = IdGenerator.randomId())
data class CausationId(val value: String = IdGenerator.randomId())
data class CorrelationId(val value: String = IdGenerator.randomId())
data class EventId(val value: String = IdGenerator.randomId())

sealed class CommandHandlingResult<C: DomainCommand, E: DomainEvent, S: AggregateState>(
        open val aggregateId: AggregateId,
        open val aggregateType: Aggregate<C,E,S>) {
    abstract val command: CommandEnvelope<C>
}
data class SuccessResult<C: DomainCommand, E: DomainEvent, S: AggregateState> (override val aggregateId: AggregateId, override val aggregateType: Aggregate<C,E,S>, override val command: CommandEnvelope<C>, val generatedEvents: List<E>, val deduplicated: Boolean = false) : CommandHandlingResult<C,E,S>(aggregateId, aggregateType)
data class RejectionResult<C: DomainCommand, E: DomainEvent, S: AggregateState>(override val aggregateId: AggregateId, override val aggregateType: Aggregate<C,E,S>,override val command: CommandEnvelope<C>, val error: Throwable, val deduplicated: Boolean = false) : CommandHandlingResult<C,E,S>(aggregateId, aggregateType)
class ConcurrentModificationResult<C: DomainCommand, E: DomainEvent, S: AggregateState>(override val aggregateId: AggregateId, override val aggregateType: Aggregate<C,E,S>,override val command: CommandEnvelope<C>) : CommandHandlingResult<C,E,S>(aggregateId, aggregateType)
class UnexpectedExceptionResult<C: DomainCommand, E: DomainEvent, S: AggregateState>(override val aggregateId: AggregateId, override val aggregateType: Aggregate<C,E,S>,override val command: CommandEnvelope<C>, val ex: Throwable): CommandHandlingResult<C,E,S>(aggregateId, aggregateType)

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
    val correlationId: CorrelationId? = null,
    val dryRun: Boolean = false // TODO: Flesh out the behaviour of dry run
)

interface AggregateRoot<C: DomainCommand, E: DomainEvent, S: AggregateState> {

    infix fun handleCommandEnvelope(commandEnvelope: CommandEnvelope<C>): Mono<CommandHandlingResult<C, E, S>>

    infix fun handleCommand(command: C): Mono<CommandHandlingResult<C, E, S>> = handleCommandEnvelope(CommandEnvelope(command))
}

interface DomainModel {

    fun <C: DomainCommand, E: DomainEvent, S: AggregateState> aggregateRootOf(
            aggregateType: Aggregate<C,E,S>,
            aggregateId: AggregateId = AggregateId()): AggregateRoot<C,E,S>
}