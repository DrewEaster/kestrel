package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.PersistedEvent
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.SynchronousJdbcReadModel
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Transaction
import com.github.andrewoma.kwery.core.Session
import com.google.inject.Inject

class SynchronousJdbcUserReadModel @Inject() constructor(val db: Database) : SynchronousJdbcReadModel, UserReadModel {

    override fun aggregateType() = User

    override suspend fun findAllUsers(): List<UserDTO> = db.withSession { session ->
        session.select("SELECT * from usr") { row ->
            UserDTO(
                id = row.string("id"),
                username = row.string("username"),
                password = row.string("password"),
                locked = row.boolean("locked")
            )
        }
    }

    suspend override fun findUserById(id: String): UserDTO? = db.withSession { session ->
        session.select("SELECT * from usr WHERE id = :id", mapOf("id" to id)) { row ->
            UserDTO(
                id = row.string("id"),
                username = row.string("username"),
                password = row.string("password"),
                locked = row.boolean("locked")
            )
        }
    }.firstOrNull()

    override fun <E : DomainEvent> update(event: PersistedEvent<E>, session: Session, tx: Transaction) {

        val rawEvent = event.rawEvent

        val userId = event.aggregateId.value

        when(rawEvent) {
            is UserRegistered -> execute(createNewUser(userId, rawEvent.username, rawEvent.password), session, tx)
            is UsernameChanged -> execute(setUsername(userId, rawEvent.username), session, tx)
            is PasswordChanged -> execute(setPassword(userId, rawEvent.password), session, tx)
            is UserLocked -> execute(setLocked(userId, true), session, tx)
            is FailedLoginAttemptsIncremented -> return
        }
    }

    private fun createNewUser(userId: String, username: String, password: String): (Session, Transaction) -> Unit = { session, tx ->
        session.update("INSERT INTO usr(id, username, password, locked) VALUES(:id, :username, :password, :locked)", mapOf(
            "id" to userId,
            "username" to username,
            "password" to password,
            "locked" to false
        ))
    }

    private fun setUsername(userId: String, username: String): (Session, Transaction) -> Unit = { session, _ ->
        session.update("UPDATE usr SET username = :username WHERE id = :id", mapOf(
            "id" to userId,
            "username" to username
        ))
    }

    private fun setPassword(userId: String, password: String): (Session, Transaction) -> Unit = { session, _ ->
        session.update("UPDATE usr SET password = :password WHERE id = :id", mapOf(
            "id" to userId,
            "password" to password
        ))
    }

    private fun setLocked(userId: String, locked: Boolean): (Session, Transaction) -> Unit = { session, _ ->
        session.update("UPDATE usr SET locked = :locked WHERE id = :id", mapOf(
            "id" to userId,
            "locked" to locked
        ))
    }
}