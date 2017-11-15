package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.kotlintest.specs.BehaviorSpec

class MyTests : BehaviorSpec() {
    init {
        given("a broomstick") {
            `when`("I sit on it") {
                then("I should be able to fly") {
                    // test code
                }
            }
            `when`("I throw it away") {
                then("it should come back") {
                    // test code
                }
            }
        }
    }
}

class DummyEvent(data: String = "") : DomainEvent {
    override val tag = DomainEventTag("dummy-event")
}

class EventWithComplexMigrationHistoryClassName3(val forename: String, val surname: String, val active: Boolean) : DomainEvent {
    override val tag = DomainEventTag("dummy-event")
}

class EventWithNoMigrationHistory(val forename: String, val surname: String, val active: Boolean) : DomainEvent {
    override val tag = DomainEventTag("dummy-event")
}

class EventWithComplexMigrationHistoryMappingConfigurer : JsonEventMappingConfigurer<EventWithComplexMigrationHistoryClassName3> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<EventWithComplexMigrationHistoryClassName3>) {
        configurationFactory.create("com.dreweaster.jester.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName1")
                .migrateFormat(migrateVersion1ToVersion2)
                .migrateFormat(migrateVersion2ToVersion3)
                .migrateFormat(migrateVersion3ToVersion4)
                .migrateClassName("com.dreweaster.jester.infrastructure.driven.backend.mapper.json.EventWithComplexMigrationHistoryClassName2")
                .migrateFormat(migrateVersion4ToVersion6)
                .migrateClassName(EventWithComplexMigrationHistoryClassName3::class.qualifiedName)
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
        configurationFactory.create(EventWithNoMigrationHistory::class.qualifiedName)
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
