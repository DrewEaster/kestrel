package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.InMemoryBackend
import kotlinx.coroutines.experimental.runBlocking

fun main(args: Array<String>) {

    val domainModel = EventSourcedDomainModel(InMemoryBackend(), TwentyFourHourWindowCommandDeduplication)
    domainModel.addReporter(ConsoleReporter)

    val aggregateRoot = domainModel.aggregateRootOf(User)

    runBlocking {
        aggregateRoot(RegisterUser("drew.easter", "password"))
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(IncrementFailedLoginAttempts)
        aggregateRoot(ChangePassword("newPassword"))
    }
}