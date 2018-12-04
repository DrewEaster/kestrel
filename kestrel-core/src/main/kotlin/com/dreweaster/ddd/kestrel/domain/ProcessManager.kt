package com.dreweaster.ddd.kestrel.domain

import com.dreweaster.ddd.kestrel.application.AggregateId
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.reflect.KClass

interface ProcessManagerContext

interface ProcessManagerState

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

sealed class SchedulableCommandAction
data class SendCommandImmediately<Cmd : ARCommand, ARCommand: DomainCommand, AREvent: DomainEvent, ARState: AggregateState> (val command: Cmd, val aggregateType: Aggregate<ARCommand, AREvent, ARState>)
data class SendCommandLater<Cmd : ARCommand, ARCommand: DomainCommand, AREvent: DomainEvent, ARState: AggregateState> (val command: Cmd, val aggregateType: Aggregate<ARCommand, AREvent, ARState>, val at: Instant)

sealed class SchedulableEventAction
data class SendEventImmediately<Evt: E, E: DomainEvent> (val event: Evt)
data class SendEventLater<Evt: E, E: DomainEvent> (val event: Evt, val at: Instant)

data class ProcessManagerStep<Result>(val callable: (() -> Result)?, val commands: List<SchedulableCommandAction>, val events: List<SchedulableEventAction>)

class CommandReceiver<C: DomainCommand, E: DomainEvent, S: AggregateState>(aggregateType: Aggregate<C, E, S>) {

    var id: AggregateId? = null

    infix fun identifiedBy(id: AggregateId): CommandReceiver<C,E,S> {
        return this
    }

    infix fun at(timestamp: Instant): CommandReceiver<C,E,S> {
        return this
    }

    infix fun after(duration: Duration): CommandReceiver<C,E,S> {
        return this
    }
}

class EventReceiver<E: DomainEvent>(event: E) {


}


class ProcessManagerBehaviour<C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState, State: S> {

    var capturedHandlers : Map<KClass<E>,((C, State, E) -> ProcessManagerStepBuilder<*, C, E, S>)> = emptyMap()

    @Suppress("UNCHECKED_CAST")
    inline fun <reified Evt: E> process(noinline handler: (C, State, Evt) -> ProcessManagerStepBuilder<*, C, E, S>) {
        capturedHandlers += Evt::class as KClass<E> to handler as (C, State, E) -> ProcessManagerStepBuilder<*, C, E, S>
    }

    fun <Result, ResultState : S> goto(state: ResultState, callable: () -> Result): ProcessManagerStepBuilder<Result,C, E, S> {
        return ProcessManagerStepBuilder(state)
    }

    fun <ResultState : S> goto(state: ResultState): ProcessManagerStepBuilder<Unit, C, E, S> {
        return ProcessManagerStepBuilder(state)
    }

    infix fun <C: DomainCommand, E: DomainEvent, S: AggregateState> C.toAggregate(aggregateType: Aggregate<C, E, S>) = CommandReceiver(aggregateType)

    fun Int.hours(): Duration {
        return Duration.of(this.toLong(), ChronoUnit.HOURS)
    }

    fun Int.minutes(): Duration {
        return Duration.of(this.toLong(), ChronoUnit.MINUTES)
    }

    infix fun E.at(timestamp: Instant): EventReceiver<E> {
        return EventReceiver(this)
    }

    infix fun E.after(duration: Duration): EventReceiver<E> {
        return EventReceiver(this)
    }

    val now = Instant.now()
}

class ProcessManagerStepBuilder<Result, C: ProcessManagerContext, E: DomainEvent, S: ProcessManagerState>(val state: S) {

    fun andSend(result: (Result) -> CommandReceiver<*,*,*>): ProcessManagerStepBuilder<Result, C, E, S> = this

    fun <Evt: E> andEmit(result: (Result) -> EventReceiver<Evt>) = this
}