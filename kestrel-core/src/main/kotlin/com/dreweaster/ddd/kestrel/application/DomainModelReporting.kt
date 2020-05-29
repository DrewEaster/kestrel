package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent

interface DomainModelReporter {

    fun <C : DomainCommand, E : DomainEvent, S: AggregateState> supports(aggregateType: Aggregate<C,E,S>): Boolean

    fun <C : DomainCommand, E : DomainEvent, S: AggregateState> createProbe(
            aggregateType: Aggregate<C,E,S>, aggregateId: AggregateId): CommandHandlingProbe<C, E, S>
}

// TODO: Would be good to add a "snapshots persisted" callback
interface CommandHandlingProbe<C : DomainCommand, E : DomainEvent, S: AggregateState> {

    fun startedHandling(command: CommandEnvelope<C>)

    fun startedRecoveringAggregate()

    fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S? = null, snapshot: Snapshot<S>?)

    fun finishedRecoveringAggregate(unexpectedException: Throwable)

    fun startedRecoveringSnapshot()

    fun finishedRecoveringSnapshot()

    fun finishedRecoveringSnapshot(unexpectedException: Throwable)

    fun finishedRecoveringSnapshot(state: S, version: Long)

    fun startedRecoveringPersistedEvents()

    fun finishedRecoveringPersistedEvents()

    fun finishedRecoveringPersistedEvents(unexpectedException: Throwable)

    fun startedApplyingCommand()

    fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean = false)

    fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean = false)

    fun commandApplicationFailed(unexpectedException: Throwable)

    fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long)

    fun finishedPersistingEvents()

    fun finishedPersistingEvents(unexpectedException: Throwable)

    fun finishedHandling(result: CommandHandlingResult<C, E, S>)
}

class ReportingContext<C : DomainCommand, E : DomainEvent, S: AggregateState>(
        aggregateType: Aggregate<C, E, S>,
        aggregateId: AggregateId,
        reporters: List<DomainModelReporter>) : CommandHandlingProbe<C, E, S> {

    private val probes: List<CommandHandlingProbe<C, E, S>> = reporters.filter { it.supports(aggregateType) }.map { it.createProbe(aggregateType, aggregateId) }

    override fun startedHandling(command: CommandEnvelope<C>) {
        probes.forEach { it.startedHandling(command) }
    }

    override fun startedRecoveringAggregate() {
        probes.forEach { it.startedRecoveringAggregate() }
    }

    override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?, snapshot: Snapshot<S>?) {
        probes.forEach { it.finishedRecoveringAggregate(previousEvents, version, state, snapshot) }
    }

    override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
        probes.forEach { probe -> probe.finishedRecoveringAggregate(unexpectedException) }
    }

    override fun startedRecoveringSnapshot() {
        probes.forEach { probe -> probe.startedRecoveringSnapshot() }
    }

    override fun finishedRecoveringSnapshot() {
        probes.forEach { probe -> probe.finishedRecoveringSnapshot() }
    }

    override fun finishedRecoveringSnapshot(unexpectedException: Throwable) {
        probes.forEach { probe -> probe.finishedRecoveringSnapshot(unexpectedException) }
    }

    override fun finishedRecoveringSnapshot(state: S, version: Long) {
        probes.forEach { probe -> probe.finishedRecoveringSnapshot(state, version) }
    }

    override fun startedRecoveringPersistedEvents() {
        probes.forEach { probe -> probe.startedRecoveringPersistedEvents() }
    }

    override fun finishedRecoveringPersistedEvents() {
        probes.forEach { probe -> probe.finishedRecoveringPersistedEvents() }
    }

    override fun finishedRecoveringPersistedEvents(unexpectedException: Throwable) {
        probes.forEach { probe -> probe.finishedRecoveringPersistedEvents(unexpectedException) }
    }

    override fun startedApplyingCommand() {
        probes.forEach { it.startedApplyingCommand() }
    }

    override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
        probes.forEach { it.commandApplicationAccepted(events, deduplicated) }
    }

    override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
        probes.forEach { it.commandApplicationRejected(rejection, deduplicated) }
    }

    override fun commandApplicationFailed(unexpectedException: Throwable) {
        probes.forEach { it.commandApplicationFailed(unexpectedException) }
    }

    override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
        probes.forEach { it.startedPersistingEvents(events, expectedSequenceNumber) }
    }

    override fun finishedPersistingEvents() {
        probes.forEach { it.finishedPersistingEvents() }
    }

    override fun finishedPersistingEvents(unexpectedException: Throwable) {
        probes.forEach { it.finishedPersistingEvents(unexpectedException) }
    }

    override fun finishedHandling(result: CommandHandlingResult<C,E,S>) {
        probes.forEach { it.finishedHandling(result) }
    }
}

object ConsoleReporter : DomainModelReporter {

    class ConsoleProbe<C : DomainCommand, E : DomainEvent, S : AggregateState> : CommandHandlingProbe<C,E,S> {

        override fun startedHandling(command: CommandEnvelope<C>) {
            println("Started handling: $command")
        }

        override fun startedRecoveringAggregate() {
            println("Started recovering aggregate")
        }

        override fun finishedRecoveringAggregate(previousEvents: List<E>, version: Long, state: S?, snapshot: Snapshot<S>?) {
            println("Successfully recovered aggregate: version = $version, events = $previousEvents, currentState = $state, snapshot = $snapshot")
        }

        override fun finishedRecoveringAggregate(unexpectedException: Throwable) {
            println("Failed to recover aggregate: $unexpectedException")
        }

        override fun startedRecoveringSnapshot() {
            println("Started recovering snapshot")
        }

        override fun finishedRecoveringSnapshot() {
            println("No snapshot exists")
        }

        override fun finishedRecoveringSnapshot(unexpectedException: Throwable) {
            println("Failed to recover snapshot: $unexpectedException")
        }

        override fun finishedRecoveringSnapshot(state: S, version: Long) {
            println("Successfully recovered snapshot with version $version: $state")
        }

        override fun startedRecoveringPersistedEvents() {
            println("Started recovering persisted events")
        }

        override fun finishedRecoveringPersistedEvents() {
            println("Successfully recovered persisted events")
        }

        override fun finishedRecoveringPersistedEvents(unexpectedException: Throwable) {
            println("Failed to recover persisted events: $unexpectedException")
        }

        override fun startedApplyingCommand() {
            println("Started applying command")
        }

        override fun commandApplicationAccepted(events: List<E>, deduplicated: Boolean) {
            println("Successfully applied command: generatedEvents = $events, deduplicated = $deduplicated")
        }

        override fun commandApplicationRejected(rejection: Throwable, deduplicated: Boolean) {
            println("Command was rejected: rejection = $rejection, deduplicated = $deduplicated")
        }

        override fun commandApplicationFailed(unexpectedException: Throwable) {
            println("Command application failed: error = $unexpectedException")
        }

        override fun startedPersistingEvents(events: List<E>, expectedSequenceNumber: Long) {
            println("Started persisting generated events: expectedVersion = $expectedSequenceNumber, events = $events")
        }

        override fun finishedPersistingEvents() {
            println("Successfully persisted events")
        }

        override fun finishedPersistingEvents(unexpectedException: Throwable) {
            println("Failed to persist events: $unexpectedException")
        }

        override fun finishedHandling(result: CommandHandlingResult<C,E,S>) {
            println("Finished command handling with result: $result")
        }
    }

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> supports(aggregateType: Aggregate<C, E, S>) = true

    override fun <C : DomainCommand, E : DomainEvent, S : AggregateState> createProbe(aggregateType: Aggregate<C, E, S>, aggregateId: AggregateId) = ConsoleProbe<C,E,S>()
}