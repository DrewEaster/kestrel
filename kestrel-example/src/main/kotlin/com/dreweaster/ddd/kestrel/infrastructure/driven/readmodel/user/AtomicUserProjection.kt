package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.AtomicDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.ResultRow
import com.google.inject.Inject

import reactor.core.publisher.Mono

class AtomicUserProjection @Inject constructor(private val database: Database): AtomicDatabaseProjection(), UserReadModel {

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

            "INSERT into usr (id, username, password, locked) VALUES ($1, $2, $3, $4)".params(
                "$1" to e.aggregateId.value,
                "$2" to e.rawEvent.username,
                "$3" to e.rawEvent.password,
                "$4" to false).expect(1)
        }

        event<UsernameChanged> { e ->

            "UPDATE usr SET username = $1 WHERE id = $2".params(
                "$1" to e.aggregateId.value,
                "$2" to e.rawEvent.username).expect(1)
        }

        event<PasswordChanged> { e ->

            "UPDATE usr SET password = $1 WHERE id = $2".params(
                "$1" to e.aggregateId.value,
                "$2" to e.rawEvent.password).expect(1)
        }

        event<UserLocked> { e ->

            "UPDATE usr SET locked = $1 WHERE id = $2".params(
                "$1" to e.aggregateId.value,
                "$2" to true).expect(1)
        }

        event<UserUnlocked> { e ->

            "UPDATE usr SET locked = $1 WHERE id = $2".params(
                "$1" to e.aggregateId.value,
                "$2" to false).expect(1)
        }
    }

    override fun findAllUsers(): Mono<List<UserDTO>> = database.inTransaction { tx ->
        tx.select("SELECT * from users") { userDtoMapper(it) }
    }.collectList()

    override fun findUserById(id: String): Mono<UserDTO?> = database.inTransaction { tx ->
        tx.select("SELECT * from usr where id = $1", "$1" to id) { userDtoMapper(it) }
    }.collectList().map { it.firstOrNull() }
}