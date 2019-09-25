package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.domain.AggregateData
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.util.json.*
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.kotlintest.specs.FeatureSpec
import io.kotlintest.matchers.shouldBe

class JsonMappingContextTests : FeatureSpec() {

    val objectMapper = ObjectMapper()

    init {
        feature("A JsonMappingContext can deserialise different versions of conceptual aggregate data with a complex migration history") {

            val mappers: List<JsonMapper<AggregateData>> =
                    listOf(EventWithComplexMigrationHistoryMapper()) as List<JsonMapper<AggregateData>>

            val context = JsonMappingContext(mappers)

            scenario("deserialise a version 1 payload") {
                // Given
                val eventVersion1Payload = jsonObject(
                    "firstName" to "joe",
                    "secondName" to "bloggs"
                )

                // When
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion1Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion2Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion3Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion4Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion5Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion6Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion7Payload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventVersion8Payload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        8)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }
        }

        feature("A JsonMappingContext can deserialise a variety of conceptual aggregate data") {

            val mappers: List<JsonMapper<AggregateData>> =
                    listOf(
                        EventWithComplexMigrationHistoryMapper(),
                        EventWithNoMigrationHistoryMapper()
                    ) as List<JsonMapper<AggregateData>>

            val context = JsonMappingContext(mappers)

            scenario("Deserialises correctly an event that has no migration history") {

                // Given
                val eventPayload = jsonObject(
                        "forename" to "joe",
                        "surname" to "bloggs",
                        "active" to true
                )

                // When
                val event = context.deserialise<EventWithNoMigrationHistory>(
                        objectMapper.writeValueAsString(eventPayload),
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
                val event = context.deserialise<EventWithComplexMigrationHistoryClassName3>(
                        objectMapper.writeValueAsString(eventPayload),
                        "com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName3",
                        8)

                // Then
                event.forename shouldBe "joe"
                event.surname shouldBe "bloggs"
                event.active shouldBe false
            }
        }

        feature("A JsonMappingContext can serialise a variety of conceptual aggregate data") {

            val mappers: List<JsonMapper<AggregateData>> =
                    listOf(
                            EventWithComplexMigrationHistoryMapper(),
                            EventWithNoMigrationHistoryMapper()
                    ) as List<JsonMapper<AggregateData>>

            val context = JsonMappingContext(mappers)

            scenario("Serialises correctly aggregate data that has no migration history") {
                // When
                val result = context.serialise(EventWithNoMigrationHistory("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = objectMapper.readTree(serialisedPayload)

                payloadAsJson["forename"].string shouldBe "joe"
                payloadAsJson["surname"].string shouldBe "bloggs"
                payloadAsJson["active"].bool shouldBe true

                result.version shouldBe 1
            }

            scenario("Serialises correctly aggregate data with migration history") {
                // When
                val result = context.serialise(EventWithComplexMigrationHistoryClassName3("joe", "bloggs", true))

                // Then
                val serialisedPayload = result.payload
                val payloadAsJson = objectMapper.readTree(serialisedPayload)
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

class EventWithComplexMigrationHistoryMapper : JsonMapper<EventWithComplexMigrationHistoryClassName3> {

    override fun configure(factory: JsonMapperBuilderFactory<EventWithComplexMigrationHistoryClassName3>) {
        factory.create("com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1")
                .migrateFormat(migrateVersion1ToVersion2)
                .migrateFormat(migrateVersion2ToVersion3)
                .migrateFormat(migrateVersion3ToVersion4)
                .migrateClassName("com.dreweaster.kestrel.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2")
                .migrateFormat(migrateVersion4ToVersion6)
                .migrateClassName(EventWithComplexMigrationHistoryClassName3::class.qualifiedName!!)
                .migrateFormat(migrateVersion6ToVersion8)
                .mappingFunctions(serialise, deserialise)
    }

    private val serialise: (EventWithComplexMigrationHistoryClassName3) -> ObjectNode = { event ->
        jsonObject(
            "forename" to event.forename,
            "surname" to event.surname,
            "active" to event.active
        )
    }

    private val deserialise: (ObjectNode) -> EventWithComplexMigrationHistoryClassName3 = { root ->
        EventWithComplexMigrationHistoryClassName3(
            forename = root["forename"].string,
            surname = root["surname"].string,
            active = root["active"].bool)
    }

    val migrateVersion1ToVersion2: (ObjectNode) -> ObjectNode = { node ->
        val firstName = node["firstName"].string
        val secondName = node["secondName"].string

        jsonObject(
            "first_name" to firstName,
            "second_name" to secondName
        )
    }

    val migrateVersion2ToVersion3: (ObjectNode) -> ObjectNode = { node ->
        val secondName = node["second_name"].string
        node - "second_name" + ("last_name" to secondName)
    }

    val migrateVersion3ToVersion4: (ObjectNode) -> ObjectNode = { node ->
        val firstName = node["first_name"].string
        val lastName = node["last_name"].string
        jsonObject(
            "forename" to firstName,
            "surname" to lastName
        )
    }

    val migrateVersion4ToVersion6: (ObjectNode) -> ObjectNode = { node ->
        node + ("activated" to true)
    }

    val migrateVersion6ToVersion8: (ObjectNode) -> ObjectNode = { node ->
        val activated = node["activated"].bool
        node - "activated" + ("active" to activated)
    }
}

class EventWithNoMigrationHistoryMapper : JsonMapper<EventWithNoMigrationHistory> {

    override fun configure(factory: JsonMapperBuilderFactory<EventWithNoMigrationHistory>) {
        factory.create(EventWithNoMigrationHistory::class.qualifiedName!!)
                .mappingFunctions(serialise, deserialise)
    }

    val serialise: (EventWithNoMigrationHistory) -> ObjectNode = { event ->
        jsonObject(
            "forename" to event.forename,
            "surname" to event.surname,
            "active" to event.active
        )
    }

    val deserialise: (ObjectNode) -> EventWithNoMigrationHistory = { root ->
        EventWithNoMigrationHistory(
            forename = root["forename"].string,
            surname = root["surname"].string,
            active = root["active"].bool)
    }
}
