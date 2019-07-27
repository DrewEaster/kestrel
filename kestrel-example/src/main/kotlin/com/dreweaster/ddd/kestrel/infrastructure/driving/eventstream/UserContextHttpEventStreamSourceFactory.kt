package com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream

import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.PasswordChangedMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.UserRegisteredMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.UsernameChangedMapper
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceFactory

object UserContextHttpEventStreamSourceFactory: BoundedContextHttpEventSourceFactory(UserContext) {

    override val mappers = eventMappers {

        tag("user-event") {
            event<UserRegistered>("com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered", UserRegisteredMapper.deserialiser)
            event<PasswordChanged>("com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged", PasswordChangedMapper.deserialiser)
            event<UsernameChanged>("com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged", UsernameChangedMapper.deserialiser)
            event<UserLocked>("com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked") { UserLocked }
            event<UserUnlocked>("com.dreweaster.ddd.kestrel.domain.aggregates.user.UserUnlocked") { UserUnlocked }
            event<FailedLoginAttemptsIncremented>("com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented") { FailedLoginAttemptsIncremented }
        }
    }
}