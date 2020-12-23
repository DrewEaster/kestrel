package com.dreweaster.ddd.kestrel

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.processmanager.stateless.HelloNewUser
import com.dreweaster.ddd.kestrel.application.processmanager.stateless.WarnUserLocked
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.reporting.micrometer.MicrometerBoundedContextHttpEventSourceReporter
import com.dreweaster.ddd.kestrel.application.reporting.micrometer.MicrometerDomainModelReporter
import com.dreweaster.ddd.kestrel.domain.Aggregate
import com.dreweaster.ddd.kestrel.domain.Persistable
import com.dreweaster.ddd.kestrel.domain.AggregateState
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.backend.PostgresBackend
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.r2dbc.R2dbcDatabase
import com.dreweaster.ddd.kestrel.infrastructure.cluster.LocalCluster
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMappingContext
import com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user.ImmediatelyConsistentUserProjection
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.events.*
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.state.ActiveUserMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.state.LockedUserMapper
import com.dreweaster.ddd.kestrel.infrastructure.driving.eventsource.KestrelExampleJsonEventMapper
import com.dreweaster.ddd.kestrel.infrastructure.driving.eventsource.UserContextHttpEventSourceFactory
import com.dreweaster.ddd.kestrel.infrastructure.driving.processmanager.stateless.HelloNewUserMaterialiser
import com.dreweaster.ddd.kestrel.infrastructure.driving.processmanager.stateless.WarnUserLockedMaterialiser
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.rdbms.offset.PostgresOffsetTracker
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.producer.BoundedContextHttpJsonEventProducer
import com.dreweaster.ddd.kestrel.infrastructure.scheduling.ClusterAwareScheduler
import com.dreweaster.ddd.kestrel.util.json.jsonArray
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.dreweaster.ddd.kestrel.util.json.obj
import com.dreweaster.ddd.kestrel.util.json.string
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.netty.handler.codec.http.HttpHeaderNames.*
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.netty.handler.codec.http.QueryStringDecoder
import io.r2dbc.client.R2dbc
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import org.flywaydb.core.Flyway
import org.reactivestreams.Publisher
import reactor.netty.http.server.HttpServer
import reactor.core.publisher.Mono
import reactor.netty.NettyOutbound
import reactor.netty.http.client.HttpClient
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import java.time.Duration
import io.r2dbc.pool.ConnectionPoolConfiguration

fun main(args: Array<String>) {
    Application.run()
}

object Application {

    private val objectMapper = ObjectMapper()

    fun run() {

        // Migrate DB
        val flyway = Flyway()
        flyway.setDataSource("jdbc:postgresql://example-db/postgres", "postgres", "password")
        flyway.migrate()

        val configuration = PostgresqlConnectionConfiguration.builder()
            .host("example-db")
            .database("postgres")
            .username("postgres")
            .password("password")
            .build()

        val poolConfiguration = ConnectionPoolConfiguration.builder(PostgresqlConnectionFactory(configuration))
            .validationQuery("SELECT 1")
            .maxIdleTime(Duration.ofMillis(1000))
            .maxSize(20)
            .build()

        val pool = ConnectionPool(poolConfiguration)

        val database = R2dbcDatabase(R2dbc(pool))


        val userEventTag = DomainEventTag("user-event")
        val jsonEventMappers = listOf(
            KestrelExampleJsonEventMapper(userEventTag, UserRegisteredMapper),
            KestrelExampleJsonEventMapper(userEventTag, UsernameChangedMapper),
            KestrelExampleJsonEventMapper(userEventTag, PasswordChangedMapper),
            KestrelExampleJsonEventMapper(userEventTag, FailedLoginAttemptsIncrementedMapper),
            KestrelExampleJsonEventMapper(userEventTag, UserLockedMapper)
        )

        val mappingContext = JsonMappingContext(jsonEventMappers.map { it.mapper as JsonMapper<Persistable> } + listOf(
                ActiveUserMapper,
                LockedUserMapper
        ) as List<JsonMapper<Persistable>>)

        val eventSourcingConfiguration = object : EventSourcingConfiguration {
            override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> commandDeduplicationThresholdFor(aggregateType: Aggregate<*, E, S>) = 100
            override fun <E : DomainEvent, S : AggregateState, A : Aggregate<*, E, S>> snapshotThresholdFor(aggregateType: Aggregate<*, E, S>) = 1
        }

        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val domainModelReporter = MicrometerDomainModelReporter(meterRegistry)
        val config = ConfigFactory.load()
        val userReadModel = ImmediatelyConsistentUserProjection(database)
        val backend = PostgresBackend(database, mappingContext, listOf(userReadModel))
        val domainModel = EventSourcedDomainModel(backend, eventSourcingConfiguration)
        domainModel.addReporter(domainModelReporter)
        val jobManager = ClusterAwareScheduler(LocalCluster)
        val offsetManager = PostgresOffsetTracker(database)

        val userContextEventSourceFactory = UserContextHttpEventSourceFactory(jsonEventMappers, mappingContext)
        val streamSourceFactories = listOf(userContextEventSourceFactory)
        val boundedContextHttpEventSourceReporter = MicrometerBoundedContextHttpEventSourceReporter(meterRegistry)
        val streamSources = BoundedContextEventSources(streamSourceFactories.map {
            it.name to it.createHttpEventSource(
                httpClient = HttpClient.create(),
                configuration = createHttpEventSourceConfiguration(it.name, config),
                jobManager = jobManager,
                offsetTracker = offsetManager
            ).addReporter(boundedContextHttpEventSourceReporter)
        })

        // Start process managers
        HelloNewUserMaterialiser(streamSources)
        WarnUserLockedMaterialiser(streamSources)

        val server = HttpServer.create()
            .host("0.0.0.0")
            .port(8080)
            .route { routes ->
                routes
                    .get("/events") { request, response ->
                        val producer = BoundedContextHttpJsonEventProducer(backend)
                        response.sendObjectAsJson(producer.produceFrom(request.queryParams()).map { it.nullable() }) { it }
                    }
                    .get("/users") { _, response ->
                        response.sendListAsJson(userReadModel.findAllUsers(), userToJsonObject)
                    }
                    .post("/users") { request, response ->
                        response.sendObjectAsJson(
                            request.receiveJsonObject(RegisterUserRequest.mapper).flatMap { registerUserRequest ->
                                val user = domainModel.aggregateRootOf(User, registerUserRequest.id)
                                user.handleCommand(RegisterUser(registerUserRequest.username, registerUserRequest.password))
                            }.flatMap { result -> when(result) {
                                is UnexpectedExceptionResult -> Mono.error(result.ex)
                                else -> Mono.just(result.aggregateId)
                            }}
                        ) { id -> jsonObject("id" to id.value)} // TODO: Error handling
                    }
                    .get("/users/{id}") { request, response ->
                        response.sendObjectAsJson(domainModel.aggregateRootOf(User, AggregateId(request.param("id")!!)).currentState() as Mono<UserState?>) { state ->
                            when(state) {
                                is ActiveUser -> jsonObject("username" to state.username)
                                is LockedUser -> jsonObject("username" to state.username)
                            }
                        }
                    }
                    .post("/users/{id}/login-attempts") { request, response ->
                        response.sendObjectAsJson(
                            request.receiveJsonObject(LoginRequest.mapper).flatMap { loginRequest ->
                                val user = domainModel.aggregateRootOf(User, AggregateId(request.param("id")!!))
                                user.handleCommand(Login(loginRequest.password))
                            }.flatMap { result -> when(result) {
                                is UnexpectedExceptionResult -> Mono.error(result.ex)
                                else -> Mono.just(result.aggregateId)
                            }}
                        ) { id -> jsonObject("id" to id.value)} // TODO: Error handling
                    }
                    .get("/metrics") { request, response ->
                        with(response) {
                            status(OK)
                            header(CONTENT_TYPE, "text/plain")
                            sendString(Mono.just(meterRegistry.scrape()))
                        }
                    }
            }.bindNow()

        server.onDispose().block()
    }

    private val userToJsonObject: (UserDTO) -> ObjectNode = { user ->
        jsonObject(
            "id" to user.id,
            "username" to user.username,
            "password" to user.password,
            "locked" to user.locked
        )
    }

    private fun createHttpEventSourceConfiguration(context: BoundedContextName, config: Config): BoundedContextHttpEventSourceConfiguration {

        return object : BoundedContextHttpEventSourceConfiguration {

            override val producerEndpointProtocol = config.getString("contexts.${context.name}.protocol")

            override val producerEndpointHostname = config.getString("contexts.${context.name}.host")

            override val producerEndpointPort = config.getInt("contexts.${context.name}.port")

            override val producerEndpointPath = config.getString("contexts.${context.name}.path")

            override fun batchSizeFor(subscriptionName: String) = config.getInt("contexts.${context.name}.subscriptions.$subscriptionName.batch_size")

            override fun repeatScheduleFor(subscriptionName: String) = Duration.ofMillis(config.getLong("contexts.${context.name}.subscriptions.$subscriptionName.repeat_schedule"))

            override fun timeoutFor(subscriptionName: String) = Duration.ofMillis(config.getLong("contexts.${context.name}.subscriptions.$subscriptionName.timeout"))

            override fun enabled(subscriptionName: String) = config.getString("contexts.${context.name}.subscriptions.$subscriptionName.enabled")?.toBoolean() ?: true

            override fun ignoreUnrecognisedEvents(subscriptionName: String) = config.getString("contexts.${context.name}.subscriptions.$subscriptionName.ignore_unrecognised_events")?.toBoolean() ?: true
        }
    }

    private fun HttpServerRequest.queryParams(): Map<String, List<String>> = QueryStringDecoder(this.uri()).parameters()

    private fun <T> HttpServerRequest.receiveJsonObject(mapper: (ObjectNode) -> T): Mono<T> {
        return this.receive().aggregate().asString().map { objectMapper.readTree(it).obj }.map(mapper)
    }

    private fun <T> HttpServerResponse.sendObjectAsJson(obj: Mono<T?>, mapper: (T) -> ObjectNode): Publisher<Void> {
        return obj.flatMap { maybeObject ->
            maybeObject?.let { obj ->
                this.status(OK)
                    .header(CONTENT_TYPE, "application/json")
                    .sendString(Mono.just(mapper(obj).toString()))
                    .then()
            } ?: this.sendNotFound()
        }
    }

    private fun <T> HttpServerResponse.sendListAsJson(objList: Mono<List<T>>, mapper: (T) -> ObjectNode): NettyOutbound {
        val jsonString = objList.map { arrayItems -> arrayItems.map { mapper(it) } }.map { jsonArray(it) }.map { it.toString() }
        return with(this) {
            status(OK)
            header(CONTENT_TYPE, "application/json")
            sendString(jsonString)
        }
    }

    private fun <T> T.nullable(): T? = this
}

data class RegisterUserRequest(val id: AggregateId, val username: String, val password: String) {
    companion object {
        val mapper: (ObjectNode) -> RegisterUserRequest = { jsonObject ->
            val username = jsonObject["username"].string
            val password = jsonObject["password"].string
            RegisterUserRequest(AggregateId(IdGenerator.randomId()), username, password)
        }
    }
}

data class LoginRequest(val password: String) {
    companion object {
        val mapper: (ObjectNode) -> LoginRequest = { jsonObject ->
            val password = jsonObject["password"].string
            LoginRequest(password)
        }
    }
}
