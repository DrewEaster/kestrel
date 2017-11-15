package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.kotlintest.specs.FeatureSpec

class JsonEventPayloadMapperTests : FeatureSpec() {

    val objectMapper = ObjectMapper()

    init {
        feature("A JsonPayloadMapper can deserialiseEvent different versions of a conceptual event with a complex migration history") {

            val configurers: List<JsonEventMappingConfigurer<DomainEvent>> =
                    listOf(EventWithComplexMigrationHistoryMappingConfigurer()) as List<JsonEventMappingConfigurer<DomainEvent>>

            val payloadMapper = JsonEventPayloadMapper(objectMapper, configurers)

            scenario("deserialise a version 1 payload") {
                // Given
                val eventVersion1Payload = objectMapper.createObjectNode()
                        .put("firstName", "joe")
                        .put("secondName", "bloggs")
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion1Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        1)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 2 payload") {
                // Given
                val eventVersion2Payload = objectMapper.createObjectNode()
                        .put("first_name", "joe")
                        .put("second_name", "bloggs")
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion2Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        2)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 3 payload") {
                // Given
                val eventVersion3Payload = objectMapper.createObjectNode()
                        .put("first_name", "joe")
                        .put("last_name", "bloggs")
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion3Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        3)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 4 payload") {
                // Given
                val eventVersion4Payload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion4Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1",
                        4)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 5 payload") {
                // Given
                val eventVersion5Payload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion5Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2",
                        5)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("deserialise a version 6 payload") {
                // Given
                val eventVersion6Payload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .put("activated", false)
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion6Payload,
                        "com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2",
                        6)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }

            scenario("deserialise a version 7 payload") {
                // Given
                val eventVersion7Payload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .put("activated", false)
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion7Payload,
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        7)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }

            scenario("deserialise a version 8 payload") {
                // Given
                val eventVersion8Payload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .put("active", false)
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventVersion8Payload,
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

            val payloadMapper = JsonEventPayloadMapper(objectMapper, configurers)

            scenario("Deserialises correctly an event that has no migration history") {

                // Given
                val eventPayload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .put("active", true)
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithNoMigrationHistory>(
                        eventPayload,
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithNoMigrationHistory",
                        1)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe true
            }

            scenario("Deserialises correctly the latest version of an event with migration history") {
                // Given
                val eventPayload = objectMapper.createObjectNode()
                        .put("forename", "joe")
                        .put("surname", "bloggs")
                        .put("active", false)
                        .toString()

                // When
                val event = payloadMapper.deserialiseEvent<EventWithComplexMigrationHistoryClassName3>(
                        eventPayload,
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

            val payloadMapper = JsonEventPayloadMapper(objectMapper, configurers)

            scenario("Serialises correctly an event that has no migration history") {
                // When
                val result = payloadMapper.serialiseEvent(EventWithNoMigrationHistory("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = objectMapper.readTree(serialisedPayload)

                payloadAsJson.get("forename").asText() shouldBe "joe"
                payloadAsJson.get("surname").asText() shouldBe "bloggs"
                payloadAsJson.get("active").asBoolean() shouldBe true

                result.version shouldBe 1
            }

            scenario("Serialises correctly an event with migration history") {
                // When
                val result = payloadMapper.serialiseEvent(EventWithComplexMigrationHistoryClassName3("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = objectMapper.readTree(serialisedPayload)
                payloadAsJson.get("forename").asText() shouldBe "joe"
                payloadAsJson.get("surname").asText() shouldBe "bloggs"
                payloadAsJson.get("active").asBoolean() shouldBe true

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

    private val serialise: (EventWithComplexMigrationHistoryClassName3, ObjectNode) -> JsonNode = { event, root ->
        root.put("forename", event.forename)
            .put("surname", event.surname)
            .put("active", event.active)
    }

    private val deserialise: (JsonNode) -> EventWithComplexMigrationHistoryClassName3 = { root ->
        EventWithComplexMigrationHistoryClassName3(
                forename = root.get("forename").asText(),
                surname = root.get("surname").asText(),
                active = root.get("active").asBoolean())
    }

    val migrateVersion1ToVersion2: (JsonNode) -> JsonNode = { node ->
        val firstName = node.get("firstName").asText()
        val secondName = node.get("secondName").asText()

        (node as ObjectNode).removeAll()
                .put("first_name", firstName)
                .put("second_name", secondName)
    }

    val migrateVersion2ToVersion3: (JsonNode) -> JsonNode = { node ->
        val secondName = node.get("second_name").asText()
        (node as ObjectNode).remove("second_name")
        node.put("last_name", secondName)
    }

    val migrateVersion3ToVersion4: (JsonNode) -> JsonNode = { node ->
        val firstName = node.get("first_name").asText()
        val lastName = node.get("last_name").asText()
        (node as ObjectNode).removeAll()
                .put("forename", firstName)
                .put("surname", lastName)
    }

    val migrateVersion4ToVersion6: (JsonNode) -> JsonNode = { node ->
        (node as ObjectNode).put("activated", true)
    }

    val migrateVersion6ToVersion8: (JsonNode) -> JsonNode = { node ->
        val activated = node.get("activated").asBoolean()
        (node as ObjectNode).remove("activated")
        node.put("active", activated)
    }
}

class EventWithNoMigrationHistoryMappingConfigurer : JsonEventMappingConfigurer<EventWithNoMigrationHistory> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<EventWithNoMigrationHistory>) {
        configurationFactory.create(EventWithNoMigrationHistory::class.qualifiedName!!)
                .mappingFunctions(serialise, deserialise)
    }

    val serialise: (EventWithNoMigrationHistory, ObjectNode) -> JsonNode = { event, root ->
        root.put("forename", event.forename)
            .put("surname", event.surname)
            .put("active", event.active)
    }

    val deserialise: (JsonNode) -> EventWithNoMigrationHistory = { root ->
        EventWithNoMigrationHistory(
                forename = root.get("forename").asText(),
                surname = root.get("surname").asText(),
                active = root.get("active").asBoolean())
    }
}
