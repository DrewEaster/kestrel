package com.dreweaster.ddd.kestrel

import com.dreweaster.ddd.kestrel.application.AggregateId
import com.dreweaster.ddd.kestrel.application.EventSourcedDomainModel
import com.dreweaster.ddd.kestrel.application.IdGenerator
import com.dreweaster.ddd.kestrel.application.TwentyFourHourWindowCommandDeduplication
import com.dreweaster.ddd.kestrel.application.consumers.HelloNewUser
import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextName
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.aggregates.user.RegisterUser
import com.dreweaster.ddd.kestrel.domain.aggregates.user.User
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.postgres.PostgresBackend
import com.dreweaster.ddd.kestrel.infrastructure.backend.rdbms.r2dbc.R2dbcDatabase
import com.dreweaster.ddd.kestrel.infrastructure.cluster.LocalCluster
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventPayloadMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user.AtomicUserProjection
import com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.*
import com.dreweaster.ddd.kestrel.infrastructure.driving.eventstream.UserContextHttpEventStreamSourceFactory
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.BoundedContextHttpEventSourceConfiguration
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.consumer.offset.PostgresOffsetTracker
import com.dreweaster.ddd.kestrel.infrastructure.http.eventstream.producer.BoundedContextHttpJsonEventStreamProducer
import com.dreweaster.ddd.kestrel.infrastructure.scheduling.ClusterAwareReactiveScheduler
import com.github.salomonbrys.kotson.jsonArray
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.netty.handler.codec.http.HttpHeaderNames.*
import io.netty.handler.codec.http.HttpResponseStatus.*
import io.netty.handler.codec.http.QueryStringDecoder
import io.r2dbc.client.R2dbc
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import org.flywaydb.core.Flyway
import reactor.netty.http.server.HttpServer
import reactor.core.publisher.Mono
import reactor.netty.NettyOutbound
import reactor.netty.http.client.HttpClient
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import java.time.Duration

object Application {

    private val jsonParser = JsonParser()

    fun main(args: Array<String>) {

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

        val database = R2dbcDatabase(R2dbc(PostgresqlConnectionFactory(configuration)))

        val payloadMapper = JsonEventPayloadMapper(Gson(), listOf(
                UserRegisteredMapper,
                UsernameChangedMapper,
                PasswordChangedMapper,
                FailedLoginAttemptsIncrementedMapper,
                UserLockedMapper
        ) as List<JsonEventMappingConfigurer<DomainEvent>>)

        val config = ConfigFactory.load()
        val userReadModel = AtomicUserProjection(database)
        val backend = PostgresBackend(database, payloadMapper, listOf(userReadModel))
        val domainModel = EventSourcedDomainModel(backend, TwentyFourHourWindowCommandDeduplication)
        val jobManager = ClusterAwareReactiveScheduler(LocalCluster)
        val offsetManager = PostgresOffsetTracker(database)

        val streamSourceFactories = listOf(UserContextHttpEventStreamSourceFactory)
        val streamSources = BoundedContextEventSources(streamSourceFactories.map {
            it.name to it.createHttpEventSource(
                httpClient = HttpClient.create(),
                configuration = createHttpEventSourceConfiguration(it.name, config),
                jobManager = jobManager,
                offsetManager = offsetManager
            )
        })

        // Start event processManager
        HelloNewUser(streamSources)

        val server = HttpServer.create()
            .route { routes ->
                routes
                    .get("/events") { request, response ->
                        val producer = BoundedContextHttpJsonEventStreamProducer(backend)
                        response.sendObjectAsJson(producer.produceFrom(request.queryParams())) { it }
                    }
                    .get("/users") { _, response ->
                        response.sendListAsJson(userReadModel.findAllUsers(), userToJsonObject)
                    }
                    .post("/users") { request, response ->
                        response.sendObjectAsJson(
                            request.receiveJsonObject(RegisterUserRequest.mapper).flatMap { registerUserRequest ->
                                val user = domainModel.aggregateRootOf(User, registerUserRequest.id)
                                user.handleCommand(RegisterUser(registerUserRequest.username, registerUserRequest.password))
                            }.flatMap { result -> Mono.just(result.aggregateId) }
                        ) { id -> jsonObject("id" to id.value)} // TODO: Error handling
                    }
                    .get("/users/{id}") { request, response ->
                        response.sendObjectAsJson(userReadModel.findUserById(request.param("id")!!).map { it!! }, userToJsonObject)
                    }
            }.bindNow()

        server.onDispose().block()
    }

    private val userToJsonObject: (UserDTO) -> JsonObject = { user ->
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

            override fun enabled(subscriptionName: String) = config.getString("contexts.${context.name}.subscriptions.$subscriptionName.enabled")?.toBoolean() ?: true
        }
    }

    private fun HttpServerRequest.queryParams(): Map<String, List<String>> = QueryStringDecoder(this.uri()).parameters()

    private fun <T> HttpServerRequest.receiveJsonObject(mapper: (JsonObject) -> T): Mono<T> {
        return this.receive().aggregate().asString().map { jsonParser.parse(it).asJsonObject }.map(mapper)
    }

    private fun <T> HttpServerResponse.sendObjectAsJson(obj: Mono<T>, mapper: (T) -> JsonObject): NettyOutbound {
        val jsonString = obj.map { mapper(it) }.map { it.toString() }
        return with(this) {
            status(OK)
            header(CONTENT_TYPE, "application/json")
            sendString(jsonString)
        }
    }

    private fun <T> HttpServerResponse.sendListAsJson(objList: Mono<List<T>>, mapper: (T) -> JsonObject): NettyOutbound {
        val jsonString = objList.map { arrayItems -> arrayItems.map { mapper(it) } }.map { jsonArray(it) }.map { it.toString() }
        return with(this) {
            status(OK)
            header(CONTENT_TYPE, "application/json")
            sendString(jsonString)
        }
    }
}

data class RegisterUserRequest(val id: AggregateId, val username: String, val password: String) {
    companion object {
        val mapper: (JsonObject) -> RegisterUserRequest = { jsonObject ->
            val username = jsonObject["username"].string
            val password = jsonObject["password"].string
            RegisterUserRequest(AggregateId(IdGenerator.randomId()), username, password)
        }
    }
}
