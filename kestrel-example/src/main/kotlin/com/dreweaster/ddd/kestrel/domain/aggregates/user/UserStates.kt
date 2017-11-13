package com.dreweaster.ddd.kestrel.domain.aggregates.user

import com.dreweaster.ddd.kestrel.domain.AggregateState

sealed class UserState : AggregateState

data class ActiveUser(val username: String, val password: String, val failedLoginAttempts: Int = 0): UserState()
data class LockedUser(val username: String, val password: String): UserState()

