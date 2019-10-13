package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.ConsistentDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.ResultRow

import reactor.core.publisher.Mono
import reactor.core.publisher.toMono

class ImmediatelyConsistentUserProjection constructor(private val database: Database): ConsistentDatabaseProjection(), UserReadModel {

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
            statement("INSERT into usr (id, username, password, locked) VALUES (:id, :username, :password, :locked)") {
                this["id"] = e.aggregateId.value
                this["username"] = e.rawEvent.username
                this["password"] = e.rawEvent.password
                this["locked"] = false
            }
        }

        event<UsernameChanged> { e ->
            statement("UPDATE usr SET username = :username WHERE id = :id") {
                this["id"] = e.aggregateId.value
                this["username"] = e.rawEvent.username
            }.expect(1)
        }

        event<PasswordChanged> { e ->
            statement("UPDATE usr SET password = :password WHERE id = :id") {
                this["id"] = e.aggregateId.value
                this["password"] = e.rawEvent.password
            }.expect(1)
        }

        event<UserLocked> { e ->
            statement("UPDATE usr SET locked = :locked WHERE id = :id") {
                this["id"] = e.aggregateId.value
                this["locked"] = true
            }.expect(1)
        }

        event<UserUnlocked> { e ->
            statement("UPDATE usr SET locked = :locked WHERE id = :id") {
                this["id"] = e.aggregateId.value
                this["locked"] = false
            }.expect(1)
        }
    }

    override fun findAllUsers(): Mono<List<UserDTO>> = database.withContext { tx ->
        tx.select("SELECT * from usr") { userDtoMapper(it) }
    }.collectList()

    override fun findUserById(id: String): Mono<UserDTO> = database.withContext { tx ->
        tx.select("SELECT * from usr where id = :id", "id" to id) { userDtoMapper(it) }
    }.toMono()
}