package com.dreweaster.ddd.kestrel.application.readmodel.user

data class UserDTO(val id: String, val username: String, val password: String, val locked: Boolean)

interface UserReadModel {

    suspend fun findAllUsers(): List<UserDTO>

    suspend fun findUserById(id: String): UserDTO?
}