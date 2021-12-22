package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.application.reporting.micrometer.MicrometerDomainModelReporter
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.IsolationMode
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

// TODO: Test Eden command and event handling
// TODO: Determine if current approach to not supporting eden commands and events outside of edenBehaviour is actually correct feature
class EventsourcedDomainModelTests : WordSpec() {

    val backend = MockBackend()

    val eventSourcingConfiguration = SwitchableEventSourcingConfiguration()

    val domainModel = EventSourcedDomainModel(backend, eventSourcingConfiguration)

    override fun isolationMode() = IsolationMode.InstancePerTest

    val meterRegistry = SimpleMeterRegistry()

    init {
        backend.clear()
        backend.toggleLoadSnapshotErrorStateOff()
        backend.toggleLoadEventsErrorStateOff()
        backend.toggleSaveErrorStateOff()
        backend.toggleOffOptimisticConcurrencyExceptionOnSave()
        eventSourcingConfiguration.toggleDeduplicationOn()
        eventSourcingConfiguration.resetSnapshotThreshold()
        domainModel.addReporter(ConsoleReporter)
        domainModel.addReporter(MicrometerDomainModelReporter(meterRegistry))

        "An AggregateRoot" should {
            "be createable for the first time" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                (result as SuccessResult).generatedEvents.size shouldBe 1
                result.generatedEvents[0] shouldBe UserRegistered(username = "joebloggs", password = "password")
                result.deduplicated shouldBe false
            }

            "be able to refer to existing state when processing a command" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // When
                val result = user.handleCommand(ChangePassword(password = "changedPassword")).block()

                // Then
                (result as SuccessResult).generatedEvents.size shouldBe 1
                result.generatedEvents[0] shouldBe PasswordChanged(oldPassword = "password", password = "changedPassword")
                result.deduplicated shouldBe false
            }

            "support emitting multiple events for a single input command" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()

                // When
                val result = user.handleCommand(Login(password = "wrongpassword")).block()

                // Then
                (result as SuccessResult).generatedEvents.size shouldBe 2
                result.generatedEvents[0] shouldBe FailedLoginAttemptsIncremented
                result.generatedEvents[1] shouldBe UserLocked
                result.deduplicated shouldBe false
            }

            "deduplicate a command with a previously handled command id and of the same command type" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                val firstResult = user.handleCommandEnvelope(CommandEnvelope(Login(password = "wrongpassword"), CommandId("command-id-5"))).block()
                val eventsGeneratedWhenCommandFirstHandled = (firstResult as SuccessResult).generatedEvents

                val existingDomainEventCount = backend.countAllEvents()

                // When
                val result = user.handleCommandEnvelope(CommandEnvelope(Login(password = "wrongpassword"), CommandId("command-id-5"))).block()

                // Then
                (result as SuccessResult).generatedEvents shouldBe eventsGeneratedWhenCommandFirstHandled
                result.deduplicated shouldBe true
                backend.countAllEvents() shouldBe existingDomainEventCount // ensure no events were persisted
            }

            "propagate error when a command is explicitly rejected in its current behaviour" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()

                // When
                val result = user.handleCommand(ChangePassword("changedPassword")).block()

                // Then
                (result as RejectionResult).error shouldBe instanceOf(UserIsLocked::class)

            }

            "generate an error when a command is not explicitly handled in its eden behaviour" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(UnlockUser).block()

                // Then
                (result as RejectionResult).error shouldBe instanceOf(UnsupportedCommandInEdenBehaviour::class)
            }

            "generate an error when a command is not explicitly handled in its current behaviour" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // When
                val result = user.handleCommand(UnlockUser).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(UnsupportedCommandInCurrentBehaviour::class)
            }


            // TODO: needs a more descriptive exception type
            "generate an error when handling a command for which there is no corresponding event supported in its current behaviour" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()

                // When
                val result = user.handleCommand(UnlockUser).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(UnsupportedEventInCurrentBehaviour::class)
            }

            "propagate error if backend fails to load events when handling a command" {
                // Given
                backend.toggleLoadEventsErrorStateOn()
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(IllegalStateException::class)
            }

            "return a ConcurrentModification result when saving to event store generates an OptimisticConcurrencyException" {
                // Given
                backend.toggleOnOptimisticConcurrencyExceptionOnSave()
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                result shouldBe instanceOf(ConcurrentModificationResult::class)
            }

            "propagate error when event store fails to save generated events" {
                // Given
                backend.toggleSaveErrorStateOn()
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(IllegalStateException::class)
            }

            "allow a duplicate command if the deduplication strategy says it's ok to allow it" {
                // Given
                eventSourcingConfiguration.toggleDeduplicationOff()
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // When
                user.handleCommandEnvelope(CommandEnvelope(ChangePassword("changedPassword"), CommandId("some-command-id"))).block()
                val result = user.handleCommandEnvelope(CommandEnvelope(ChangePassword("anotherChangedPassword"), CommandId("some-command-id"))).block()

                // Then
                (result as SuccessResult).generatedEvents.size shouldBe 1
                result.generatedEvents[0] shouldBe PasswordChanged(oldPassword = "changedPassword", password = "anotherChangedPassword")
                result.deduplicated shouldBe false
            }

            // TODO: It's likely too restrictive to prevent eden commands from being used in other behaviours
            "reject an attempt to send a eden command twice" {
                // Given
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(AggregateInstanceAlreadyExists::class)
            }

            "creates expected snapshots and successfully restores from latest snapshot" {

                fun verifyNoSnapshot(aggregateId: String) {
                    backend.loadSnapshot(User, AggregateId(aggregateId)).block() shouldBe null
                }

                fun verifySnapshot(aggregateId: String, expectedVersion: Long, expectedState: UserState) {
                    // The snapshot should have been persisted at version 3
                    val snapshot = backend.loadSnapshot(User, AggregateId(aggregateId)).block()!!
                    snapshot.version shouldBe expectedVersion
                    snapshot.state shouldBe expectedState
                }

                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                verifyNoSnapshot("some-aggregate-id")

                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                verifyNoSnapshot("some-aggregate-id")

                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                verifyNoSnapshot("some-aggregate-id")

                user.handleCommand(Login(password = "wrongpassword")).block()
                verifyNoSnapshot("some-aggregate-id")

                user.handleCommand(ChangePassword(password = "changedPassword2")).block()
                verifySnapshot("some-aggregate-id", 3L, ActiveUser("changedUsername1", "changedPassword1", 1))

                // Clearing events covered by expected snapshot
                backend.clearEvents { it.second < 4 }

                user.handleCommand(ChangePassword(password = "changedPassword3")).block()
                verifySnapshot("some-aggregate-id", 3L, ActiveUser("changedUsername1", "changedPassword1", 1))

                user.handleCommand(ChangePassword(password = "changedPassword4")).block()
                verifySnapshot("some-aggregate-id", 3L, ActiveUser("changedUsername1", "changedPassword1", 1))

                user.handleCommand(ChangePassword(password = "changedPassword5")).block()
                verifySnapshot("some-aggregate-id", 3L, ActiveUser("changedUsername1", "changedPassword1", 1))

                user.handleCommand(ChangePassword(password = "changedPassword6")).block()
                verifySnapshot("some-aggregate-id", 7L, ActiveUser("changedUsername1", "changedPassword5", 1))

                // Clearing events covered by expected snapshot
                backend.clearEvents { it.second < 8 }

                user.handleCommand(ChangePassword(password = "changedPassword7")).block()
                verifySnapshot("some-aggregate-id", 7L, ActiveUser("changedUsername1", "changedPassword5", 1))

                user.handleCommand(ChangePassword(password = "changedPassword8")).block()
                verifySnapshot("some-aggregate-id", 7L, ActiveUser("changedUsername1", "changedPassword5", 1))

                user.handleCommand(ChangePassword(password = "changedPassword9")).block()
                verifySnapshot("some-aggregate-id", 7L, ActiveUser("changedUsername1", "changedPassword5", 1))

                user.handleCommand(ChangePassword(password = "changedPassword10")).block()
                verifySnapshot("some-aggregate-id", 11L, ActiveUser("changedUsername1", "changedPassword9", 1))

                // Clearing events covered by expected snapshot
                backend.clearEvents { it.second < 12 }

                user.handleCommand(ChangePassword(password = "changedPassword11")).block()
                verifySnapshot("some-aggregate-id", 11L, ActiveUser("changedUsername1", "changedPassword9", 1))

                user.handleCommand(ChangePassword(password = "changedPassword12")).block()
                verifySnapshot("some-aggregate-id", 11L, ActiveUser("changedUsername1", "changedPassword9", 1))

                val (userState, version) = user.currentState().block()

                // And the user state should be as expected
                (userState as ActiveUser).username shouldBe "changedUsername1"
                version shouldBe 14L
                userState.password shouldBe "changedPassword12"
                userState.failedLoginAttempts shouldBe 1
            }

            "propagate error if backend fails to load snapshot when handling a command" {
                // Given
                backend.toggleLoadSnapshotErrorStateOn()
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                // When
                val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(IllegalStateException::class)
            }

            // stateAt() - No snapshot

            "return empty for state at version when there's no snapshot and no events for the aggregate" {
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                val result = user.stateAt(0L).block()
                result shouldBe null
            }

            "restore state at version when there's no snapshot and full event history exists up to the the requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(Int.MAX_VALUE)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                val result = user.stateAt(4L).block()
                result shouldBe ActiveUser("changedUsername1", "changedPassword2", 1)
            }

            "return empty state at version when there's no snapshot and full event history exists, but not up to the the requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(Int.MAX_VALUE)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                val result = user.stateAt(5L).block()
                result shouldBe null
            }

            "throw error when there's no snapshot and event history does not start from sequence number 0" {
                eventSourcingConfiguration.setSnapshotThreshold(Int.MAX_VALUE)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear the first event in the history
                backend.clearEvents { it.second < 1 }

                shouldThrow<EventHistoryCorrupted> { user.stateAt(5L).block() }
            }

            // stateAt() - Snapshot version > requested version

            "return empty state at version when there's a snapshot beyond requested version, but no events for the aggregate" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear all the events
                backend.clearEvents { it.second < 5 }

                val result = user.stateAt(2L).block()
                result shouldBe null
            }

            "restore state at version when there's a snapshot beyond requested version and full event history exists up to the the requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                val result = user.stateAt(2L).block()
                result shouldBe ActiveUser("changedUsername1", "changedPassword1", 0)
            }

            "return empty state at version when there's a snapshot beyond requested version and full event history exists, but not up to the the requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                backend.clearEvents { it.second > 1 }

                val result = user.stateAt(2L).block()
                result shouldBe null
            }

            "throw error when there's a snapshot beyond requested version, but event history does not start from sequence number 0" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear the first event in the history
                backend.clearEvents { it.second < 1 }

                shouldThrow<EventHistoryCorrupted> { user.stateAt(2L).block() }
            }

            // stateAt() - Snapshot version == requested version
            "return state at version when snapshot version equals requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear all the events so that we can prove we're fetching only from the snapshot
                backend.clearEvents { it.second < 5 }

                val result = user.stateAt(3L).block()
                result shouldBe ActiveUser("changedUsername1", "changedPassword1", 1)
            }

            // stateAt() - Snapshot version < requested version
            "return empty state at version when there's a snapshot before requested version, but there are no events after snapshot version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear all events
                backend.clearEvents { it.second < 5 }

                val result = user.stateAt(4L).block()
                result shouldBe null
            }

            // stateAt() - Snapshot version < requested version
            "return empty state at version when there's a snapshot before requested version, but last event sequence number is less than requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear all the events before the snapshot so that we can prove we're fetching only from the snapshot
                backend.clearEvents { it.second < 4 }

                val result = user.stateAt(5L).block()
                result shouldBe null
            }

            "restore state at version when there's a snapshot before requested version and full event history exists up to the the requested version" {
                eventSourcingConfiguration.setSnapshotThreshold(4)
                val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                user.handleCommand(RegisterUser(username = "joebloggs", password = "password")).block()
                user.handleCommand(ChangePassword(password = "changedPassword1")).block()
                user.handleCommand(ChangeUsername(username = "changedUsername1")).block()
                user.handleCommand(Login(password = "wrongpassword")).block()
                user.handleCommand(ChangePassword(password = "changedPassword2")).block()

                // Clear all the events before the snapshot so that we can prove we're fetching only from the snapshot
                backend.clearEvents { it.second < 4 }

                val result = user.stateAt(4L).block()
                result shouldBe ActiveUser("changedUsername1", "changedPassword2", 1)
            }
        }
    }
}

class SwitchableEventSourcingConfiguration : EventSourcingConfiguration {

    private var deduplicationEnabled = true

    private var snapshotThreshold: Int = Int.MAX_VALUE

    fun toggleDeduplicationOn() {
        deduplicationEnabled = true
    }

    fun toggleDeduplicationOff() {
        deduplicationEnabled = false
    }

    fun setSnapshotThreshold(snapshotThreshold: Int) {
        this.snapshotThreshold = snapshotThreshold

    }
    fun resetSnapshotThreshold() {
        snapshotThreshold = Int.MAX_VALUE
    }

    override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> commandDeduplicationThresholdFor(aggregateType: Aggregate<*, E, S>): Int {
        return when(deduplicationEnabled) {
            true -> Int.MAX_VALUE
            false -> 0
        }
    }

    override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> snapshotThresholdFor(aggregateType: Aggregate<*, E, S>): Int {
        return snapshotThreshold
    }
}

class MockBackend : InMemoryBackend() {

    private var loadEventsErrorState = false

    private var loadSnapshotErrorState = false

    private var saveErrorState = false

    var optimisticConcurrencyExceptionOnSave = false

    fun toggleOnOptimisticConcurrencyExceptionOnSave() {
        optimisticConcurrencyExceptionOnSave = true
    }

    fun toggleOffOptimisticConcurrencyExceptionOnSave() {
        optimisticConcurrencyExceptionOnSave = false
    }

    fun toggleLoadEventsErrorStateOn() {
        loadEventsErrorState = true
    }

    fun toggleLoadEventsErrorStateOff() {
        loadEventsErrorState = false
    }

    fun toggleLoadSnapshotErrorStateOn() {
        loadSnapshotErrorState = true
    }

    fun toggleLoadSnapshotErrorStateOff() {
        loadSnapshotErrorState = false
    }

    fun toggleSaveErrorStateOn() {
        saveErrorState = true
    }

    fun toggleSaveErrorStateOff() {
        saveErrorState = false
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId): Flux<PersistedEvent<E>> {
        if (loadEventsErrorState) {
            return Flux.error(IllegalStateException())
        }
        return super.loadEvents(aggregateType, aggregateId)
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId, afterSequenceNumber: Long, toSequenceNumber: Long?): Flux<PersistedEvent<E>> {
        if (loadEventsErrorState) {
            return Flux.error(IllegalStateException())
        }
        return super.loadEvents(aggregateType, aggregateId, afterSequenceNumber, toSequenceNumber)
    }

    override fun <S : AggregateState, A : Aggregate<*, *, S>> loadSnapshot(aggregateType: A, aggregateId: AggregateId): Mono<Snapshot<S>> {
        if (loadSnapshotErrorState) {
            return Mono.error(IllegalStateException())
        }
        return super.loadSnapshot(aggregateType, aggregateId)
    }

    override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> saveEvents(
            aggregateType: A,
            aggregateId: AggregateId,
            causationId: CausationId,
            rawEvents: List<E>,
            expectedSequenceNumber: Long,
            correlationId: CorrelationId?,
            snapshot: Snapshot<S>?): Flux<PersistedEvent<E>> {

        return when {
            optimisticConcurrencyExceptionOnSave -> Flux.error(OptimisticConcurrencyException)
            saveErrorState -> Flux.error(IllegalStateException())
            else -> super.saveEvents(aggregateType, aggregateId, causationId, rawEvents, expectedSequenceNumber, correlationId, snapshot)
        }
    }
}