package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import io.kotlintest.matchers.be
import io.kotlintest.specs.WordSpec
import kotlinx.coroutines.runBlocking

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
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                    // When
                    val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // Then
                    (result as SuccessResult).generatedEvents.size shouldBe 1
                    result.generatedEvents[0] shouldBe UserRegistered(username = "joebloggs", password = "password")
                    result.deduplicated shouldBe false
                }
            }

            "be able to refer to existing state when processing a command" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // When
                    val result = user.handleCommand(ChangePassword(password = "changedPassword"))

                    // Then
                    (result as SuccessResult).generatedEvents.size shouldBe 1
                    result.generatedEvents[0] shouldBe PasswordChanged(oldPassword = "password", password = "changedPassword")
                    result.deduplicated shouldBe false
                }
            }

            "support emitting multiple events for a single input command" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)

                    // When
                    val result = user.handleCommand(IncrementFailedLoginAttempts)

                    // Then
                    (result as SuccessResult).generatedEvents.size shouldBe 2
                    result.generatedEvents[0] shouldBe FailedLoginAttemptsIncremented
                    result.generatedEvents[1] shouldBe UserLocked
                    result.deduplicated shouldBe false
                }
            }

            "deduplicate a command with a previously handled command id and of the same command type" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    val firstResult = user.handleCommandEnvelope(CommandEnvelope(IncrementFailedLoginAttempts, CommandId("command-id-5")))
                    val eventsGeneratedWhenCommandFirstHandled = (firstResult as SuccessResult).generatedEvents

                    // When
                    val result = user.handleCommandEnvelope(CommandEnvelope(IncrementFailedLoginAttempts, CommandId("command-id-5")))

                    // Then
                    (result as SuccessResult).generatedEvents shouldBe eventsGeneratedWhenCommandFirstHandled
                    result.deduplicated shouldBe true
                }
            }

            "propagate error when a command is explicitly rejected in its current behaviour" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)

                    // When
                    val result = user.handleCommand(ChangePassword("changedPassword"))

                    // Then
                    (result as RejectionResult).error should be a(UserIsLocked::class)
                }
            }

            "generate an error when a command is not explicitly handled in its current behaviour" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // When
                    val result = user.handleCommand(UnlockUser)

                    // Then
                    (result as RejectionResult).error should be a(UnsupportedCommandInCurrentBehaviour::class)
                }
            }


            // TODO: needs a more descriptive exception type
            "generate an error when handling a command for which there is no corresponding event supported in its current behaviour" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)
                    user.handleCommand(IncrementFailedLoginAttempts)

                    // When
                    user.handleCommand(UnlockUser)
                    val result = user.handleCommand(IncrementFailedLoginAttempts)

                    // Then
                    (result as UnexpectedExceptionResult).ex should be a(UnsupportedEventInCurrentBehaviour::class)
                }
            }

            "propagate error if backend fails to load events when handling a command" {
                runBlocking {
                    // Given
                    backend.toggleLoadErrorStateOn()
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                    // When
                    val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // Then
                    (result as UnexpectedExceptionResult).ex should be a(IllegalStateException::class)
                }
            }

            "return a ConcurrentModification result when saving to event store generates an OptimisticConcurrencyException" {
                runBlocking {
                    // Given
                    backend.toggleOnOptimisticConcurrencyExceptionOnSave()
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                    // When
                    val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // Then
                    result should be a(ConcurrentModificationResult::class)
                }
            }

            "propagate error when event store fails to save generated events" {
                runBlocking {
                    // Given
                    backend.toggleSaveErrorStateOn()
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))

                    // When
                    val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // Then
                    (result as UnexpectedExceptionResult).ex should be a(IllegalStateException::class)
                }
            }

            "process a duplicate command if the deduplication strategy says it's ok to process it" {
                runBlocking {
                    // Given
                    deduplicationStrategyFactory.toggleDeduplicationOff()
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // When
                    user.handleCommandEnvelope(CommandEnvelope(ChangePassword("changedPassword"), CommandId("some-command-id")))
                    val result = user.handleCommandEnvelope(CommandEnvelope(ChangePassword("anotherChangedPassword"), CommandId("some-command-id")))

                    // Then
                    (result as SuccessResult).generatedEvents.size shouldBe 1
                    result.generatedEvents[0] shouldBe PasswordChanged(oldPassword = "changedPassword", password = "anotherChangedPassword")
                    result.deduplicated shouldBe false
                }
            }

            // TODO: It's likely too restrictive to prevent eden commands from being used in other behaviours
            "reject an attempt to send a eden command twice" {
                runBlocking {
                    // Given
                    val user = domainModel.aggregateRootOf(User, AggregateId("some-aggregate-id"))
                    user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // When
                    val result = user.handleCommand(RegisterUser(username = "joebloggs", password = "password"))

                    // Then
                    (result as RejectionResult).error should be a(AggregateInstanceAlreadyExists::class)
                }
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

    suspend override fun <E : DomainEvent> loadEvents(aggregate: Aggregate<*, E, *>, aggregateId: AggregateId): List<PersistedEvent<E>> {
        if (loadErrorState) {
            throw IllegalStateException()
        }
        return super.loadEvents(aggregate, aggregateId)
    }

    suspend override fun <E : DomainEvent> saveEvents(aggregateType: Aggregate<*, E, *>, aggregateId: AggregateId, causationId: CausationId, rawEvents: List<E>, expectedSequenceNumber: Long, correlationId: CorrelationId?): List<PersistedEvent<E>> =
        when {
            optimisticConcurrencyExceptionOnSave -> throw OptimisticConcurrencyException
            saveErrorState -> throw IllegalStateException()
            else -> super.saveEvents(aggregateType, aggregateId, causationId, rawEvents, expectedSequenceNumber, correlationId)
        }
}