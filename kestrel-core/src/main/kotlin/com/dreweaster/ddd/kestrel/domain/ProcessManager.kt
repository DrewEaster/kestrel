package com.dreweaster.ddd.kestrel.domain

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.DomainModel
import io.vavr.control.Try
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.reflect.KClass

interface ProcessManagerContext

interface ProcessManagerState

class Suspend: RuntimeException {

    val failureCode: String

    constructor (failureCode: String) : super() { this.failureCode = failureCode }
    constructor (failureCode: String, message: String) : super(message) { this.failureCode = failureCode }
    constructor (failureCode: String, message: String, cause: Throwable) : super(message, cause) { this.failureCode = failureCode }
}

abstract class ProcessManagerSuspendState() : ProcessManagerState

interface ProcessManager<C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState> {

    val blueprint: ProcessManagerBlueprint<C,E,S>

    fun processManager(name: String, startWith: S, init: ProcessManagerBlueprint<C,E,S>.() -> Unit): ProcessManagerBlueprint<C,E,S> {
        val processManager = ProcessManagerBlueprint<C,E,S>(name, startWith)
        processManager.init()
        return processManager
    }
}

class ProcessManagerBlueprint<C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState>(val name: String, val startWith: S) {

    var capturedBehaviours: Map<KClass<S>, ProcessManagerBehaviour<C,E,S,*>> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified State: S> behaviour(init: ProcessManagerBehaviour<C,E,S,State>.() -> Unit): ProcessManagerBehaviour<C,E,S,State> {
        val behaviour = ProcessManagerBehaviour<C,E,S,State>()
        behaviour.init()
        capturedBehaviours += State::class as KClass<S> to behaviour
        return behaviour
    }
}

interface CommandDispatcher {

    suspend fun <C : DomainCommand, E : DomainEvent, S : AggregateState> dispatch(
            command: C,
            aggregateType: Aggregate<C, E, S>,
            aggregateId: AggregateId): Try<Unit>
}

data class SendableCommand<Cmd : ARCommand, ARCommand: DomainCommand, AREvent: DomainEvent, ARState: AggregateState> (val command: Cmd, val aggregateType: Aggregate<ARCommand, AREvent, ARState>, val id: AggregateId) {

    suspend fun sendUsing(dispatcher: CommandDispatcher) {
        dispatcher.dispatch(command, aggregateType, id)
    }
}

data class SendCommandLater<Cmd : ARCommand, ARCommand: DomainCommand, AREvent: DomainEvent, ARState: AggregateState> (val command: Cmd, val aggregateType: Aggregate<ARCommand, AREvent, ARState>, val id: AggregateId, val at: Instant)

sealed class SchedulableEventAction
data class SendEventImmediately<Evt: E, E: DomainEvent> (val event: Evt): SchedulableEventAction()
data class SendEventLater<Evt: E, E: DomainEvent> (val event: Evt, val at: Instant): SchedulableEventAction()

class ProcessManagerBehaviour<C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState, State: S> {

    var capturedHandlers : Map<KClass<E>,((C, State, E) -> ProcessManagerStepBuilder<*, C, E, S>)> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Evt: E> process(noinline handler: (C, State, Evt) -> ProcessManagerStepBuilder<*, C, E, S>) {
        capturedHandlers += Evt::class as KClass<E> to handler as (C, State, E) -> ProcessManagerStepBuilder<*, C, E, S>
    }

    fun <Result, ResultState : S> goto(state: ResultState, callable: () -> Result): ProcessManagerStepBuilder<Result,C, E, S> {
        return ProcessManagerStepBuilder(state, callable)
    }

    fun <ResultState : S> goto(state: ResultState): ProcessManagerStepBuilder<Unit, C, E, S> {
        return ProcessManagerStepBuilder(state, null)
    }

    infix fun <C: DomainCommand, E: DomainEvent, S: AggregateState> C.toAggregate(aggregateType: Aggregate<C, E, S>) = CommandReceiver(this, aggregateType)

    infix fun E.at(timestamp: Instant): EventReceiver<E> {
        return EventReceiver(this, timestamp)
    }

    infix fun E.after(duration: Duration): EventReceiver<E> {
        return EventReceiver(this, duration)
    }

    val now = Instant.MIN

    fun Int.hours(): Duration {
        return Duration.of(this.toLong(), ChronoUnit.HOURS)
    }

    fun Int.minutes(): Duration {
        return Duration.of(this.toLong(), ChronoUnit.MINUTES)
    }
}

sealed class ExecutedStep
data class SuccessfullyExecutedStep(val sendableCommands: List<SendableCommand<*,*,*,*>>, val scheduledEvents: List<SchedulableEventAction>): ExecutedStep()
data class UnsuccessfullyExecutedStep(val executionException: Throwable): ExecutedStep()

class ProcessManagerStepBuilder<Result, C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState>(val state: S, val callable: (() -> Result)?) {

    var capturedCommandReceivers : List<(Result) -> CommandReceiver<*,*,*>> = emptyList()

    var capturedEventReceivers : List<(Result) -> EventReceiver<*>> = emptyList()

    fun andSend(result: (Result) -> CommandReceiver<*,*,*>): ProcessManagerStepBuilder<Result, C, E, S> {
        capturedCommandReceivers += result
        return this
    }

    fun <Evt: E> andEmit(result: (Result) -> EventReceiver<Evt>): ProcessManagerStepBuilder<Result, C, E, S> {
        capturedEventReceivers += result
        return this
    }

    fun execute(): ExecutedStep {
        val tryResult = callable?.let { Try.of(callable) }
        return if(tryResult != null) {
            when(tryResult) {
                is Try.Success -> {
                    val schedulableCommands = capturedCommandReceivers.map {
                        val commandReceiver = it.invoke(tryResult.get())
                        SendableCommand(
                            commandReceiver.command,
                            commandReceiver.aggregateType as Aggregate<DomainCommand,DomainEvent,AggregateState>,
                            commandReceiver.capturedId!!
                        )
                    }

                    val scheduledEvents = capturedEventReceivers.map {
                        val eventReceiver = it.invoke(tryResult.get())
                        if(eventReceiver.capturedTimestamp == Instant.MIN) {
                            SendEventImmediately(eventReceiver.capturedEvent as DomainEvent)
                        } else {
                            SendEventLater(
                                eventReceiver.capturedEvent as DomainEvent,
                                if(eventReceiver.capturedTimestamp != null) eventReceiver.capturedTimestamp!! else  Instant.now() + eventReceiver.capturedDuration!!
                            )
                        }
                    }

                    SuccessfullyExecutedStep(schedulableCommands, scheduledEvents)
                }
                else -> UnsuccessfullyExecutedStep(tryResult.cause)
            }
        } else {
            val schedulableCommands = capturedCommandReceivers.map {
                val commandReceiver = it.invoke(Unit as Result)
                SendableCommand(
                    commandReceiver.command,
                    commandReceiver.aggregateType as Aggregate<DomainCommand,DomainEvent,AggregateState>,
                    commandReceiver.capturedId!!
                )
            }

            val scheduledEvents = capturedEventReceivers.map {
                val eventReceiver = it.invoke(Unit as Result)
                if(eventReceiver.capturedTimestamp == Instant.MIN) {
                    SendEventImmediately(eventReceiver.capturedEvent as DomainEvent)
                } else {
                    SendEventLater(
                            eventReceiver.capturedEvent as DomainEvent,
                            if(eventReceiver.capturedTimestamp != null) eventReceiver.capturedTimestamp!! else  Instant.now() + eventReceiver.capturedDuration!!
                    )
                }
            }
            SuccessfullyExecutedStep(schedulableCommands, scheduledEvents)
        }
    }
}

class CommandReceiver<C: DomainCommand, E: DomainEvent, S: AggregateState>(val command: C, val aggregateType: Aggregate<C, E, S>) {

    var capturedId: AggregateId? = null

    infix fun identifiedBy(id: AggregateId): CommandReceiver<C,E,S> {
        capturedId = id
        return this
    }
}

class EventReceiver<E: DomainEvent>() {

    var capturedEvent: E? = null

    var capturedTimestamp: Instant? = null

    var capturedDuration: Duration? = null

    constructor (event: E, timestamp: Instant) : this() {
        capturedEvent = event
        capturedTimestamp = timestamp
    }

    constructor (event: E, duration: Duration) : this() {
        capturedEvent = event
        capturedDuration = duration
    }
}