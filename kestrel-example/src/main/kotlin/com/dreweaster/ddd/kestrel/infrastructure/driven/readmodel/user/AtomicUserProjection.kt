package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.AtomicDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultRow

import reactor.core.publisher.Mono

class AtomicUserProjection constructor(private val database: Database): AtomicDatabaseProjection(), UserReadModel {

    private val userDtoMapper: (ResultRow) -> UserDTO = {
        UserDTO(
            id = it["id"].string,
            username = it["username"].string,
            password = it["password"].string,
            locked = it["locked"].bool
        )
    }

    override val update = projection<User, UserEvent> {

        event<UserRegistered> { e ->
            statement("INSERT into usr (id, username, password, locked) VALUES ($1, $2, $3, $4)") {
                this["$1"] = e.aggregateId.value
                this["$2"] = e.rawEvent.username
                this["$3"] = e.rawEvent.password
                this["$4"] = false
            }
        }

        event<UsernameChanged> { e ->
            statement("UPDATE usr SET username = $1 WHERE id = $2") {
                this["$1"] = e.aggregateId.value
                this["$2"] = e.rawEvent.username
            }.expect(1)
        }

        event<PasswordChanged> { e ->
            statement("UPDATE usr SET password = $1 WHERE id = $2") {
                this["$1"] = e.aggregateId.value
                this["$2"] = e.rawEvent.password
            }.expect(1)
        }

        event<UserLocked> { e ->
            statement("UPDATE usr SET locked = $1 WHERE id = $2") {
                this["$1"] = e.aggregateId.value
                this["$2"] = true
            }.expect(1)
        }

        event<UserUnlocked> { e ->
            statement("UPDATE usr SET locked = $1 WHERE id = $2") {
                this["$1"] = e.aggregateId.value
                this["$2"] = false
            }.expect(1)
        }
    }

    override fun findAllUsers(): Mono<List<UserDTO>> = database.withContext { tx ->
        tx.select("SELECT * from users") { userDtoMapper(it) }
    }.collectList()

    override fun findUserById(id: String): Mono<UserDTO?> = database.withContext { tx ->
        tx.select("SELECT * from usr where id = $1", "$1" to id) { userDtoMapper(it) }
    }.collectList().map { it.firstOrNull() }
}