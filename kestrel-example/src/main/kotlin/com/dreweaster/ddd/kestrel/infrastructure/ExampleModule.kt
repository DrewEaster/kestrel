package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.PostgresBackend
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.SynchronousJdbcReadModel
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventPayloadMapper
import com.dreweaster.ddd.kestrel.infrastructure.readmodel.user.SynchronousJdbcUserReadModel
import com.dreweaster.ddd.kestrel.infrastructure.serialisation.user.*
import com.github.andrewoma.kwery.core.dialect.PostgresDialect
import com.google.gson.Gson
import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.multibindings.Multibinder
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

class ExampleModule : AbstractModule() {

    override fun configure() {
        // Read models
        val readModelsBinder = Multibinder.newSetBinder(binder(), SynchronousJdbcReadModel::class.java)
        registerReadModel<UserReadModel, SynchronousJdbcUserReadModel>(SynchronousJdbcUserReadModel::class.java, readModelsBinder)
    }

    @Singleton
    @Provides
    fun createDomainModel(backend: Backend): DomainModel {
        val domainModel = EventSourcedDomainModel(backend, TwentyFourHourWindowCommandDeduplication)
        domainModel.addReporter(ConsoleReporter)
        return domainModel
    }

    @Singleton
    @Provides
    fun provideDatabase(): Database {
        val config = HikariConfig()
        config.setJdbcUrl("jdbc:postgresql://localhost/postgres")
        config.setUsername("postgres")
        config.setPassword("password")
        config.addDataSourceProperty("cachePrepStmts", "true")
        config.addDataSourceProperty("prepStmtCacheSize", "250")
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

        val ds = HikariDataSource(config)

        return Database("dbPool", ds, ds.maximumPoolSize, PostgresDialect())
    }

    @Singleton
    @Provides
    fun provideBackend(
            database: Database,
            synchronousJdbcReadModels: java.util.Set<SynchronousJdbcReadModel>): Backend {

        val payloadMapper = JsonEventPayloadMapper(Gson(), listOf(
            UserRegisteredMapper,
            UsernameChangedMapper,
            PasswordChangedMapper,
            FailedLoginAttemptsIncrementedMapper,
            UserLockedMapper
        ) as List<JsonEventMappingConfigurer<DomainEvent>>)

        return PostgresBackend(database, payloadMapper, synchronousJdbcReadModels.toList())
    }

    private inline fun <reified T, S: T> registerReadModel(clazz: Class<S>, binder: Multibinder<SynchronousJdbcReadModel>) {
        binder.addBinding().to(SynchronousJdbcUserReadModel::class.java)
        bind(clazz).`in`(Singleton::class.java)
        bind(T::class.java).to(clazz)
    }
}