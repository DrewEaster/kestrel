package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import kotlinx.coroutines.experimental.runBlocking

fun main(args: Array<String>) {

    val backend = InMemoryBackend()
    val domainModel = EventSourcedDomainModel(backend, TwentyFourHourWindowCommandDeduplicationStrategyFactory())

    val aggregateRoot = domainModel.aggregateRootOf(User)

    runBlocking {
        aggregateRoot(RegisterUser("drew.easter", "password"))
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)

        val result = aggregateRoot(ChangePassword("newPassword"))

        when(result) {
            is SuccessResult -> println("Success: ${result.generatedEvents}, deduplicated: ${result.deduplicated}")
            is RejectionResult -> println("Rejection: ${result.error}, deduplicated: ${result.deduplicated}")
            is ConcurrentModificationResult -> println("Concurrent modification")
            is UnexpectedExceptionResult -> println("Unexpected exception: ${result.ex}")
        }
    }
}