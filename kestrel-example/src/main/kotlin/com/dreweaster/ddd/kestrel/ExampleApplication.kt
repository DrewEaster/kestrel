package com.dreweaster.ddd.kestrel

import com.dreweaster.ddd.kestrel.infrastructure.ExampleModule
import com.google.inject.Guice
import io.ktor.application.Application
import io.ktor.server.engine.commandLineEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import org.flywaydb.core.Flyway

fun Application.module() {
    Guice.createInjector(ExampleModule(this))
}

fun main(args: Array<String>) {

    // Migrate DB
    val flyway = Flyway()
    flyway.setDataSource("jdbc:postgresql://example-db/postgres", "postgres", "password")
    flyway.migrate()

    embeddedServer(Netty, commandLineEnvironment(args)).start()
}
