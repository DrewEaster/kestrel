package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.AtomicDatabaseProjection
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.DB
import com.google.inject.Inject
import io.r2dbc.spi.Row

import reactor.core.publisher.Mono

class AtomicUserProjection @Inject constructor(private val database: DB): AtomicDatabaseProjection(), UserReadModel {

    private val userDtoMapper: (Row) -> UserDTO = {
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
                e.aggregateId.value,
                e.rawEvent.username,
                e.rawEvent.password,
                false).expect(1)
        }

        event<UsernameChanged> { e ->

            "UPDATE usr SET username = $1 WHERE id = $2".params(
                e.aggregateId.value,
                e.rawEvent.username).expect(1)
        }

        event<PasswordChanged> { e ->

            "UPDATE usr SET password = $1 WHERE id = $2".params(
                e.aggregateId.value,
                e.rawEvent.password).expect(1)
        }

        event<UserLocked> { e ->

            "UPDATE usr SET locked = $1 WHERE id = $2".params(
                e.aggregateId.value,
                true).expect(1)
        }

        event<UserUnlocked> { e ->

            "UPDATE usr SET locked = $1 WHERE id = $2".params(
                e.aggregateId.value,
                false).expect(1)
        }
    }

    override fun findAllUsers(): Mono<List<UserDTO>> = database.inTransaction { tx ->
        tx.query("SELECT * from users") { userDtoMapper(it) }
    }.collectList()

    override fun findUserById(id: String): Mono<UserDTO?> = database.inTransaction { tx ->
        tx.query("SELECT * from usr where id = $1", id) { userDtoMapper(it) }
    }.collectList().map { it.firstOrNull() }
}