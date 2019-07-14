package com.dreweaster.ddd.kestrel.application.readmodel.user

import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.DatabaseReadModel
import reactor.core.publisher.Mono

data class UserDTO(val id: String, val username: String, val password: String, val locked: Boolean)

interface UserReadModel : DatabaseReadModel {

    fun findAllUsers(): Mono<List<UserDTO>>

    fun findUserById(id: String): Mono<UserDTO?>
}