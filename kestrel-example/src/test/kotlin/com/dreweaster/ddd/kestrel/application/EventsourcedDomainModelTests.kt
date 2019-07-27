package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import io.kotlintest.matchers.instanceOf
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import reactor.core.publisher.Flux

// TODO: Test Eden command and event handling
// TODO: Determine if current approach to not supporting eden commands and events outside of edenBehaviour is actually correct feature
class EventsourcedDomainModelTests : WordSpec() {

    val backend = MockBackend()

    val deduplicationStrategyFactory = SwitchableDeduplicationStrategyFactory()

    val domainModel = EventSourcedDomainModel(backend, deduplicationStrategyFactory)

    override val oneInstancePerTest = true

    init {
        backend.clear()
        backend.toggleLoadErrorStateOff()
        backend.toggleSaveErrorStateOff()
        backend.toggleOffOptimisticConcurrencyExceptionOnSave()
        deduplicationStrategyFactory.toggleDeduplicationOn()

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

                // When
                val result = user.handleCommandEnvelope(CommandEnvelope(Login(password = "wrongpassword"), CommandId("command-id-5"))).block()

                // Then
                (result as SuccessResult).generatedEvents shouldBe eventsGeneratedWhenCommandFirstHandled
                result.deduplicated shouldBe true
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
                user.handleCommand(UnlockUser).block()
                val result = user.handleCommand(Login(password = "wrongpassword")).block()

                // Then
                (result as UnexpectedExceptionResult).ex shouldBe instanceOf(UnsupportedEventInCurrentBehaviour::class)
            }

            "propagate error if backend fails to load events when handling a command" {
                // Given
                backend.toggleLoadErrorStateOn()
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
                deduplicationStrategyFactory.toggleDeduplicationOff()
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
        }
    }
}

class SwitchableDeduplicationStrategyFactory : CommandDeduplicationStrategyFactory {

    private var deduplicationEnabled = true

    fun toggleDeduplicationOn() {
        deduplicationEnabled = true
    }

    fun toggleDeduplicationOff() {
        deduplicationEnabled = false
    }

    override fun newBuilder(): CommandDeduplicationStrategyBuilder =
        object : CommandDeduplicationStrategyBuilder {
            private var causationIds: Set<CausationId> = emptySet()

            override fun addEvent(domainEvent: PersistedEvent<*>): CommandDeduplicationStrategyBuilder {
                causationIds += domainEvent.causationId
                return this
            }

            override fun build(): CommandDeduplicationStrategy =
                object : CommandDeduplicationStrategy {
                    override fun isDuplicate(commandId: CommandId) =
                        if (deduplicationEnabled) causationIds.contains(CausationId(commandId.value)) else false
                }
        }
}

class MockBackend : InMemoryBackend() {

    private var loadErrorState = false

    private var saveErrorState = false

    var optimisticConcurrencyExceptionOnSave = false

    fun toggleOnOptimisticConcurrencyExceptionOnSave() {
        optimisticConcurrencyExceptionOnSave = true
    }

    fun toggleOffOptimisticConcurrencyExceptionOnSave() {
        optimisticConcurrencyExceptionOnSave = false
    }

    fun toggleLoadErrorStateOn() {
        loadErrorState = true
    }

    fun toggleLoadErrorStateOff() {
        loadErrorState = false
    }

    fun toggleSaveErrorStateOn() {
        saveErrorState = true
    }

    fun toggleSaveErrorStateOff() {
        saveErrorState = false
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> loadEvents(aggregateType: A, aggregateId: AggregateId): Flux<PersistedEvent<E>> {
        if (loadErrorState) {
            return Flux.error(IllegalStateException())
        }
        return super.loadEvents(aggregateType, aggregateId)
    }

    override fun <E : DomainEvent, A : Aggregate<*, E, *>> saveEvents(aggregateType: A, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): Flux<PersistedEvent<E>> {
        return when {
            optimisticConcurrencyExceptionOnSave -> Flux.error(OptimisticConcurrencyException)
            saveErrorState -> Flux.error(IllegalStateException())
            else -> super.saveEvents(aggregateType, aggregateId, causationId, rawEvents, expectedSequenceNumber, correlationId)
        }
    }
}