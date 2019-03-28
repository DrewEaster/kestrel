package com.dreweaster.ddd.kestrel.infrastructure

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.consumers.HelloNewUser
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextEventStreamSources
import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.Database
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.PostgresBackend
import com.dreweaster.ddd.kestrel.infrastructure.backend.jdbc.SynchronousJdbcReadModel
import com.dreweaster.ddd.kestrel.infrastructure.cluster.ClusterManager
import com.dreweaster.ddd.kestrel.infrastructure.cluster.LocalClusterManager
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventPayloadMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user.SynchronousJdbcUserReadModel
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.*
import com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream.UserContextHttpEventStreamSourceFactory
import com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes.user.UserRoutes
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventStreamSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.OffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetManager
import com.dreweaster.ddd.kestrel.infrastructure.job.ScheduledExecutorServiceJobManager
import com.github.andrewoma.kwery.core.dialect.PostgresDialect
import com.google.gson.Gson
import com.google.inject.AbstractModule
import com.google.inject.Binder
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.multibindings.Multibinder
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.Application
import io.ktor.config.ApplicationConfig
import kotlinx.coroutines.Dispatchers
import org.asynchttpclient.DefaultAsyncHttpClient
import java.time.Duration
import java.util.concurrent.Executors

class ExampleModule(val application: Application) : AbstractModule() {

    inner class SynchronousJdbcReadModelBinder(val binder: Binder) {
        val readModelsBinder = Multibinder.newSetBinder(binder(), SynchronousJdbcReadModel::class.java)
        inline fun <reified R : SynchronousJdbcReadModel> bind(): Class<R> {
            readModelsBinder.addBinding().to(R::class.java)
            binder.bind(R::class.java).`in`(Singleton::class.java)
            return R::class.java
        }
    }

    override fun configure() {
        // Read models
        val readModelBinder = SynchronousJdbcReadModelBinder(binder())
        bind(UserReadModel::class.java).to(readModelBinder.bind<SynchronousJdbcUserReadModel>())

        // Bind routes
        bind(UserRoutes::class.java).asEagerSingleton()

        // Bind Ktor Application
        bind(Application::class.java).toInstance(application)

        // Bind Ktor configuration
        bind(ApplicationConfig::class.java).toInstance(application.environment.config)

        // Bind cluster manager
        bind(ClusterManager::class.java).toInstance(LocalClusterManager)

        // Bind consumers
        bind(HelloNewUser::class.java).asEagerSingleton()
    }

    @Singleton
    @Provides
    fun jobManager(clusterManager: ClusterManager): JobManager {
        return ScheduledExecutorServiceJobManager(
            clusterManager = clusterManager,
            scheduler = Executors.newSingleThreadScheduledExecutor()
        )
    }

    @Singleton
    @Provides
    fun offsetManager(database: Database): OffsetManager {
        return PostgresOffsetManager(database)
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
    fun boundedContextEventStreamSources(jobManager: JobManager, offsetManager: OffsetManager, config: ApplicationConfig): BoundedContextEventStreamSources {
        val asyncHttpClient = DefaultAsyncHttpClient()
        val streamSourceFactories = listOf(UserContextHttpEventStreamSourceFactory)
        return BoundedContextEventStreamSources(streamSourceFactories.map {
            it.name to it.createHttpEventStreamSource(
                httpClient = asyncHttpClient,
                configuration = createHttpEventStreamSourceConfiguration(it.name, config),
                jobManager = jobManager,
                offsetManager = offsetManager)
                    .addReporter(com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.reporting.ConsoleReporter)
        })
    }

    @Singleton
    @Provides
    fun provideDatabase(): Database {
        val config = HikariConfig()
        config.jdbcUrl ="jdbc:postgresql://example-db/postgres"
        config.username = "postgres"
        config.password = "password"
        config.addDataSourceProperty("cachePrepStmts", "true")
        config.addDataSourceProperty("prepStmtCacheSize", "250")
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

        val ds = HikariDataSource(config)

        return Database("dbPool", ds, Dispatchers.IO)
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

    private fun createHttpEventStreamSourceConfiguration(context: BoundedContextName, config: ApplicationConfig): BoundedContextHttpEventStreamSourceConfiguration {

        return object : BoundedContextHttpEventStreamSourceConfiguration {

            override val producerEndpointProtocol = config.property("contexts.${context.name}.protocol").getString()

            override val producerEndpointHostname = config.property("contexts.${context.name}.host").getString()

            override val producerEndpointPort = config.property("contexts.${context.name}.port").getString().toInt()

            override val producerEndpointPath = config.property("contexts.${context.name}.path").getString()

            override fun batchSizeFor(subscriptionName: String) = config.property("contexts.${context.name}.subscriptions.$subscriptionName.batch_size").getString().toInt()

            override fun repeatScheduleFor(subscriptionName: String) = Duration.ofMillis(config.property("contexts.${context.name}.subscriptions.$subscriptionName.repeat_schedule").getString().toLong())

            override fun enabled(subscriptionName: String) = config.propertyOrNull("contexts.${context.name}.subscriptions.$subscriptionName.enabled")?.getString()?.toBoolean() ?: true
        }
    }
}