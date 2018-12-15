package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.SynchronousJdbcReadModel
import com.google.inject.Inject
import org.jetbrains.exposed.sql.*

class SynchronousJdbcUserReadModel @Inject() constructor(private val db: Database) : SynchronousJdbcReadModel(), UserReadModel {

    object Users : Table("usr") {
        val id: Column<String> = varchar("id", 72)
        val username: Column<String> = varchar("username", 100)
        val password: Column<String> = varchar("password", 20)
        val locked: Column<Boolean> = bool("locked")
    }

    override suspend fun findAllUsers(): List<UserDTO> = db.transaction { Users.selectAll().map(rowMapper) }

    override suspend fun findUserById(id: String): UserDTO? = db.transaction { Users.select { Users.id.eq(id) }.map(rowMapper).firstOrNull() }

    override val update = projection<User, UserEvent> {

        event<UserRegistered> { _, e ->
            Users.insert {
                it[id] = e.aggregateId.value
                it[username] = e.rawEvent.username
                it[password] = e.rawEvent.password
                it[locked] = false
            }
        }

        event<UsernameChanged> { tx, e ->
            tx.assert(rowsAffected = 1) {
                Users.update({ Users.id eq e.aggregateId.value }) {
                    it[username] = e.rawEvent.username
                }
            }
        }

        event<PasswordChanged> { tx, e ->
            tx.assert(rowsAffected = 1) {
                Users.update({ Users.id eq e.aggregateId.value }) {
                    it[password] = e.rawEvent.password
                }
            }
        }

        event<UserLocked> { tx, e ->
            tx.assert(rowsAffected = 1) {
                Users.update({ Users.id eq e.aggregateId.value }) {
                    it[locked] = true
                }
            }
        }

        event<UserUnlocked> { tx, e ->
            tx.assert(rowsAffected = 1) {
                Users.update({ Users.id eq e.aggregateId.value }) {
                    it[locked] = false
                }
            }
        }
    }

    private val rowMapper: (ResultRow) -> UserDTO = { row ->
        UserDTO(
            id = row[Users.id],
            username = row[Users.username],
            password = row[Users.password],
            locked = row[Users.locked])
    }
}