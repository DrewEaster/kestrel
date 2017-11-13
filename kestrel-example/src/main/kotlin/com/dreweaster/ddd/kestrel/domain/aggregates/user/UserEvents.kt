package com.dreweaster.ddd.kestrel.domain.aggregates.user

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag

sealed class UserEvent : DomainEvent {
    override val tag = DomainEventTag("user-event")
}

data class UserRegistered(val username: String, val password: String): UserEvent()
data class PasswordChanged(val password: String): UserEvent()
data class UsernameChanged(val username: String): UserEvent()
object UserLocked: UserEvent()
object FailedLoginAttemptsIncremented : UserEvent()