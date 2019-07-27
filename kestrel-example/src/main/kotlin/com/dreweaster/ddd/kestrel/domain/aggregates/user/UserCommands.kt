package com.dreweaster.ddd.kestrel.domain.aggregates.user

import com.dreweaster.ddd.kestrel.domain.DomainCommand

sealed class UserCommand : DomainCommand

data class RegisterUser(val username: String, val password: String): UserCommand()
data class ChangePassword(val password: String): UserCommand()
data class ChangeUsername(val username: String): UserCommand()
data class Login(val password: String): UserCommand()
object UnlockUser: UserCommand()
