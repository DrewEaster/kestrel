package com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream

import com.dreweaster.ddd.kestrel.application.consumers.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.PasswordChangedMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.UserRegisteredMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.UsernameChangedMapper
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.HttpJsonEventMapper

object UserContextHttpEventStreamSourceFactory: BoundedContextHttpEventStreamSourceFactory(UserContext) {

    private val userEventTag = DomainEventTag("user-event")

    private val userRegisteredMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered",
            sourceEventTag = userEventTag,
            targetEventClass = UserRegistered::class,
            map = UserRegisteredMapper.deserialiser)

    private val passwordChangedMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged",
            sourceEventTag = userEventTag,
            targetEventClass = PasswordChanged::class,
            map = PasswordChangedMapper.deserialiser)

    private val usernameChangedMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged",
            sourceEventTag = userEventTag,
            targetEventClass = UsernameChanged::class,
            map = UsernameChangedMapper.deserialiser)

    private val userLockedMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked",
            sourceEventTag = userEventTag,
            targetEventClass = UserLocked::class) { UserLocked }

    private val userUnlockedMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserUnlocked",
            sourceEventTag = userEventTag,
            targetEventClass = UserUnlocked::class) { UserUnlocked }

    private val failedLoginAttemptsIncrementedMapper = HttpJsonEventMapper(
            sourceEventType = "com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented",
            sourceEventTag = userEventTag,
            targetEventClass = FailedLoginAttemptsIncremented::class) { FailedLoginAttemptsIncremented }

    override val mappers = listOf(
        userRegisteredMapper,
        passwordChangedMapper,
        usernameChangedMapper,
        userLockedMapper,
        userUnlockedMapper,
        failedLoginAttemptsIncrementedMapper)
}