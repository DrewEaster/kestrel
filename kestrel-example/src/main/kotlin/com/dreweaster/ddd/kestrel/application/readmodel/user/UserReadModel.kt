package com.dreweaster.ddd.kestrel.application.readmodel.user

import reactor.core.publisher.Mono

data class UserDTO(val id: String, val username: String, val password: String, val locked: Boolean)

interface UserReadModel {

    fun findAllUsers(): Mono<List<UserDTO>>

    fun findUserById(id: String): Mono<UserDTO>
}