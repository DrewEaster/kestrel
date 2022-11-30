package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.google.gson.Gson
import io.kotlintest.specs.FeatureSpec
import com.github.salomonbrys.kotson.*
import com.google.gson.JsonObject
import com.google.gson.JsonParser


class JsonEventPayloadMapperTests : FeatureSpec() {

    val gson = Gson()

    val jsonParser = JsonParser()

    init {
        feature("A JsonPayloadMapper can deserialiseEvent different versions of a conceptual event with a complex migration history") {

            val configurers: List<JsonEventMappingConfigurer<DomainEvent>> =
                    listOf(EventWithComplexMigrationHistoryMappingConfigurer()) as List<JsonEventMappingConfigurer<DomainEvent>>

            val payloadMapper = JsonEventPayloadMapper(gson, configurers)

            scenario("deserialise a version 1 payload") {

                // Given
                val eventVersion1Payload = jsonObject(
                    "firstName" to "joe",
                    "secondName" to "bloggs"
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion1Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        1)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 2 payload") {
                // Given
                val eventVersion2Payload = jsonObject(
                        "first_name" to "joe",
                        "second_name" to "bloggs"
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion2Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        2)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 3 payload") {
                // Given
                val eventVersion3Payload = jsonObject(
                        "first_name" to "joe",
                        "last_name" to "bloggs"
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion3Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        3)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 4 payload") {
                // Given
                val eventVersion4Payload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs"
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion4Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        4)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 5 payload") {
                // Given
                val eventVersion5Payload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs"
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion5Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2",
                        5)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 6 payload") {
                // Given
                val eventVersion6Payload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "activated" to false
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion6Payload),
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2",
                        6)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }

            scenario("deserialise a version 7 payload") {
                // Given
                val eventVersion7Payload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "activated" to false
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion7Payload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        7)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }

            scenario("deserialise a version 8 payload") {
                // Given
                val eventVersion8Payload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "active" to false
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventVersion8Payload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        8)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }
        }

        feature("A JsonPayloadMapper can deserialise multiple conceptual events") {

            val configurers: List<JsonEventMappingConfigurer<DomainEvent>> =
                    listOf(
                        EventWithComplexMigrationHistoryMappingConfigurer(),
                        EventWithNoMigrationHistoryMappingConfigurer()
                    ) as List<JsonEventMappingConfigurer<DomainEvent>>

            val payloadMapper = JsonEventPayloadMapper(gson, configurers)

            scenario("Deserialises correctly an event that has no migration history") {

                // Given
                val eventPayload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "active" to true
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithNoMigrationHistory>(
                        gson.toJson(eventPayload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithNoMigrationHistory",
                        1)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("Deserialises correctly the latest version of an event with migration history") {
                // Given
                val eventPayload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "active" to false
                )

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        gson.toJson(eventPayload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        8)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }
        }

        feature("A JsonPayloadMapper can serialise multiple conceptual events") {

            val configurers: List<JsonEventMappingConfigurer<DomainEvent>> =
                    listOf(
                            EventWithComplexMigrationHistoryMappingConfigurer(),
                            EventWithNoMigrationHistoryMappingConfigurer()
                    ) as List<JsonEventMappingConfigurer<DomainEvent>>

            val payloadMapper = JsonEventPayloadMapper(gson, configurers)

            scenario("Serialises correctly an event that has no migration history") {
                // When
                val result = payloadMapper.serialiseEvent(EventWithNoMigrationHistory("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = jsonParser.parse(serialisedPayload).asJsonObject

                payloadAsJson["forename"].string shouldBe "joe"
                payloadAsJson["surname"].string shouldBe "bloggs"
                payloadAsJson["active"].bool shouldBe true

                result.version shouldBe 1
            }

            scenario("Serialises correctly an event with migration history") {
                // When
                val result = payloadMapper.serialiseEvent(EventWithComplexMigrationHistoryClassName3("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = jsonParser.parse(serialisedPayload)
                payloadAsJson["forename"].string shouldBe "joe"
                payloadAsJson["surname"].string shouldBe "bloggs"
                payloadAsJson["active"].bool shouldBe true

                result.version shouldBe 8
            }
        }
    }
}

class EventWithComplexMigrationHistoryClassName3(val forename: String, val surname: String, val active: Boolean) : DomainEvent {
    override val tag = DomainEventTag("dummy-event")
}

class EventWithNoMigrationHistory(val forename: String, val surname: String, val active: Boolean) : DomainEvent {
    override val tag = DomainEventTag("dummy-event")
}

class EventWithComplexMigrationHistoryMappingConfigurer : JsonEventMappingConfigurer<EventWithComplexMigrationHistoryClassName3> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<EventWithComplexMigrationHistoryClassName3>) {
        configurationFactory.create("com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1")
                .migrateFormat(migrateVersion1ToVersion2)
                .migrateFormat(migrateVersion2ToVersion3)
                .migrateFormat(migrateVersion3ToVersion4)
                .migrateClassName("com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2")
                .migrateFormat(migrateVersion4ToVersion6)
                .migrateClassName(EventWithComplexMigrationHistoryClassName3::class.qualifiedName!!)
                .migrateFormat(migrateVersion6ToVersion8)
                .mappingFunctions(serialise, deserialise)
    }

    private val serialise: (EventWithComplexMigrationHistoryClassName3) -> JsonObject = { event ->
        jsonObject(
                "forename" to event.forename,
                "surname" to event.surname,
                "active" to event.active
        )
    }

    private val deserialise: (JsonObject) -> EventWithComplexMigrationHistoryClassName3 = { root ->
        EventWithComplexMigrationHistoryClassName3(
                forename = root["forename"].string,
                surname = root["surname"].string,
                active = root["active"].bool)
    }

    val migrateVersion1ToVersion2: (JsonObject) -> JsonObject = { node ->
        val firstName = node["firstName"].string
        val secondName = node["secondName"].string

        jsonObject(
                "first_name" to firstName,
                "second_name" to secondName
        )
    }

    val migrateVersion2ToVersion3: (JsonObject) -> JsonObject = { node ->
        val secondName = node["second_name"].string
        node -= "second_name"
        node += "last_name" to secondName
        node
    }

    val migrateVersion3ToVersion4: (JsonObject) -> JsonObject = { node ->
        val firstName = node["first_name"].string
        val lastName = node["last_name"].string
        jsonObject(
                "forename" to firstName,
                "surname" to lastName
        )
    }

    val migrateVersion4ToVersion6: (JsonObject) -> JsonObject = { node ->
        node += "activated" to true
        node
    }

    val migrateVersion6ToVersion8: (JsonObject) -> JsonObject = { node ->
        val activated = node["activated"].bool
        node -= "activated"
        node += "active" to activated
        node
    }
}

class EventWithNoMigrationHistoryMappingConfigurer : JsonEventMappingConfigurer<EventWithNoMigrationHistory> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<EventWithNoMigrationHistory>) {
        configurationFactory.create(EventWithNoMigrationHistory::class.qualifiedName!!)
                .mappingFunctions(serialise, deserialise)
    }

    val serialise: (EventWithNoMigrationHistory) -> JsonObject = { event ->
        jsonObject(
            "forename" to event.forename,
            "surname" to event.surname,
            "active" to event.active
        )
    }

    val deserialise: (JsonObject) -> EventWithNoMigrationHistory = { root ->
        EventWithNoMigrationHistory(
                forename = root["forename"].string,
                surname = root["surname"].string,
                active = root["active"].bool)
    }
}
