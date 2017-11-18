package com.dreweaster.ddd.kestrel

import com.dreweaster.ddd.kestrel.application.DomainModel
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.ChangePassword
import com.dreweaster.ddd.kestrel.domain.aggregates.user.IncrementFailedLoginAttempts
import com.dreweaster.ddd.kestrel.domain.aggregates.user.RegisterUser
import com.dreweaster.ddd.kestrel.domain.aggregates.user.User
import com.dreweaster.ddd.kestrel.infrastructure.ExampleModule
import com.google.inject.Guice
import com.google.inject.Injector
import kotlinx.coroutines.experimental.runBlocking
import org.flywaydb.core.Flyway

fun main(args: Array<String>) {
    ExampleApplication.migrateDb()
    ExampleApplication.run(Guice.createInjector(ExampleModule()))
}

object ExampleApplication {

    fun migrateDb() {
        val flyway = Flyway()
        flyway.setDataSource("jdbc:postgresql://localhost/postgres", "postgres", "password")
        flyway.migrate()
    }

    fun run(injector: Injector) {
        val domainModel = injector.getInstance(DomainModel::class.java)
        val userReadModel = injector.getInstance(UserReadModel::class.java)

        val aggregateRoot = domainModel.aggregateRootOf(User)

        runBlocking {
            aggregateRoot(RegisterUser("drew.easter", "password"))
            aggregateRoot(IncrementFailedLoginAttempts)
            aggregateRoot(IncrementFailedLoginAttempts)
            aggregateRoot(IncrementFailedLoginAttempts)
            aggregateRoot(IncrementFailedLoginAttempts)
            aggregateRoot(ChangePassword("newPassword"))
        }

        runBlocking {
            userReadModel.findAllUsers().forEach { it ->
                println(it)
            }
        }
    }
}