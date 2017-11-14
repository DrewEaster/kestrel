package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.application.EventPayloadMapper
import com.dreweaster.ddd.kestrel.application.MappingException
import com.dreweaster.ddd.kestrel.application.PayloadSerialisationResult
import com.dreweaster.ddd.kestrel.application.SerialisationContentType
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.IOException

interface JsonEventMappingConfiguration<E : DomainEvent> {

    fun migrateFormat(migration: ((JsonNode) -> JsonNode)?): JsonEventMappingConfiguration<E>

    fun migrateClassName(className: String?): JsonEventMappingConfiguration<E>

    fun mappingFunctions(serialiseFunction: ((E, ObjectNode) ->  JsonNode)?, deserialiseFunction: ((JsonNode) -> E)?)
}

interface JsonEventMappingConfigurationFactory<E : DomainEvent> {

    fun create(initialEventClassName: String?): JsonEventMappingConfiguration<E>
}

interface JsonEventMappingConfigurer<E : DomainEvent> {

    fun configure(configurationFactory: JsonEventMappingConfigurationFactory<E>)
}

class InvalidMappingConfigurationException(val configurationError: ConfigurationError) : MappingException("Invalid mapper configuration") {

    enum class ConfigurationError {
        SERIALISE_FUNCTION,
        DESERIALISE_FUNCTION,
        INITIAL_CLASS_NAME,
        MIGRATION_CLASS_NAME,
        MIGRATION_FUNCTION
    }
}

class UnparseableJsonPayloadException(cause: Throwable, serialisedPayload: String) : MappingException("Could not parse JSON event payload: " + serialisedPayload, cause)

class MissingDeserialiserException(serialisedEventType: String, serialisedEventVersion: Int) : MappingException(
        "No deserialiser found for event_type = '$serialisedEventType' with event_version = '$serialisedEventVersion'")

class MissingSerialiserException(eventType: String) : MappingException("No serialiser found for event_type = '$eventType'")


class JsonEventPayloadMapper(
        private val objectMapper: ObjectMapper,
        private val eventMappers: List<JsonEventMappingConfigurer<in DomainEvent>>) : EventPayloadMapper {

    private var eventDeserialisers: Map<Pair<String, Int>, (String) -> DomainEvent> = emptyMap()
    private var eventSerialisers: Map<String, (DomainEvent) -> Pair<String, Int>> = emptyMap()

    init {
        init(eventMappers)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <E : DomainEvent> deserialiseEvent(serialisedPayload: String, serialisedEventType: String, serialisedEventVersion: Int): E {
        val deserialiser = eventDeserialisers[Pair(serialisedEventType, serialisedEventVersion)] ?: throw MissingDeserialiserException(serialisedEventType, serialisedEventVersion)
        return deserialiser(serialisedPayload) as E
    }

    override fun <E : DomainEvent> serialiseEvent(event: E): PayloadSerialisationResult {
        val serialiser = eventSerialisers[event::class.qualifiedName!!] ?: throw MissingSerialiserException(event::class.qualifiedName!!)

        val versionedPayload = serialiser(event)

        return PayloadSerialisationResult(
                versionedPayload.first,
                SerialisationContentType.JSON,
                versionedPayload.second
        )
    }

    private fun init(configurers: List<JsonEventMappingConfigurer<DomainEvent>>) {
        // TODO: Validate no clashes between registered mappers
        // e.g. what if two mappers try to convert to the same event class?
        // e.g. what if a com.dreweaster.ddd.jester.infrastructure.driven.eventstore.com.dreweaster.ddd.jester.infrastructure.driven.eventstore.postgres.db.migration in one mapper maps to a class name in another mapper?
        // Such scenarios should be made impossible (at least for v1...)

        val mappingConfigurations = configurers.map {
            val mappingConfiguration = MappingConfiguration<DomainEvent>()
            it.configure(mappingConfiguration)
            mappingConfiguration
        }

        eventDeserialisers = mappingConfigurations.fold(eventDeserialisers) { acc, mappingConfiguration -> acc + mappingConfiguration.createDeserialisers() }

        eventSerialisers = mappingConfigurations.fold(eventSerialisers) { acc, mappingConfiguration -> acc + mappingConfiguration.createSerialiser() }
    }

    inner class MappingConfiguration<E : DomainEvent> : JsonEventMappingConfigurationFactory<E>, JsonEventMappingConfiguration<E> {

        private var currentVersion: Int = 0

        private var currentClassName: String? = null

        private var migrations: List<Migration> = emptyList()

        private var serialiseFunction: ((E, ObjectNode) -> JsonNode)? = null

        private var deserialiseFunction: ((JsonNode) -> E)? = null

        override fun migrateFormat(migration: ((JsonNode) -> JsonNode)?): JsonEventMappingConfiguration<E> {
            if (migration == null) {
                throw InvalidMappingConfigurationException(InvalidMappingConfigurationException.ConfigurationError.MIGRATION_FUNCTION)
            }

            migrations += FormatMigration(currentClassName!!, currentVersion, currentVersion + 1, migration)
            currentVersion += 1
            return this
        }

        override fun migrateClassName(className: String?): JsonEventMappingConfiguration<E> {
            if (className == null) {
                throw InvalidMappingConfigurationException(InvalidMappingConfigurationException.ConfigurationError.MIGRATION_CLASS_NAME)
            }

            val migration = ClassNameMigration(currentClassName!!, className, currentVersion, currentVersion + 1)
            migrations += migration
            currentClassName = migration.toClassName()
            currentVersion = migration.toVersion()
            return this
        }

        override fun mappingFunctions(serialiseFunction: ((E, ObjectNode) -> JsonNode)?, deserialiseFunction: ((JsonNode) -> E)?) {
            if (serialiseFunction == null) {
                throw InvalidMappingConfigurationException(InvalidMappingConfigurationException.ConfigurationError.SERIALISE_FUNCTION)
            }

            if (deserialiseFunction == null) {
                throw InvalidMappingConfigurationException(InvalidMappingConfigurationException.ConfigurationError.DESERIALISE_FUNCTION)
            }

            this.serialiseFunction = serialiseFunction
            this.deserialiseFunction = deserialiseFunction
        }

        override fun create(initialEventClassName: String?): JsonEventMappingConfiguration<E> {
            if (initialEventClassName == null) {
                throw InvalidMappingConfigurationException(InvalidMappingConfigurationException.ConfigurationError.INITIAL_CLASS_NAME)
            }
            currentClassName = initialEventClassName
            currentVersion = 1
            return this
        }

        @Suppress("UNCHECKED_CAST")
        fun createSerialiser(): Pair<String, (DomainEvent) -> Pair<String, Int>> {
            return Pair(currentClassName!!) { domainEvent ->
                val root = objectMapper.createObjectNode()
                val serialisedJsonEvent = serialiseFunction!!(domainEvent as E, root)
                Pair(serialisedJsonEvent.toString(), currentVersion)
            }
        }

        fun createDeserialisers(): Map<Pair<String, Int>, (String) -> DomainEvent> {
            var deserialisers: Map<Pair<String, Int>, (String) -> DomainEvent> = emptyMap()

            if (!migrations.isEmpty()) {
                deserialisers = putDeserialisers(migrations, deserialisers)
            }

            // Include the 'current' version deserialiser
            deserialisers += (Pair(currentClassName!!, currentVersion) to { serialisedEvent ->
                val root = stringToJsonNode(serialisedEvent)
                deserialiseFunction!!(root)
            })
            return deserialisers
        }

        private fun putDeserialisers(
                migrations: List<Migration>,
                deserialisers: Map<Pair<String, Int>, (String) -> DomainEvent>): Map<Pair<String, Int>, (String) -> DomainEvent> {

            return if (migrations.isEmpty()) {
                deserialisers
            } else {
                putDeserialisers(migrations.drop(1), putDeserialiser(migrations, deserialisers))
            }
        }

        private fun putDeserialiser(
                migrations: List<Migration>,
                deserialisers: Map<Pair<String, Int>, (String) -> DomainEvent>): Map<Pair<String, Int>, (String) -> DomainEvent> {

            val migration = migrations.first()
            val className = migration.fromClassName()
            val version = migration.fromVersion()
            val migrationFunctions = migrations.map { it.migrationFunction() }
            val combinedMigrationFunction = migrationFunctions.drop(1).fold(migrationFunctions.first()) { combined, f -> f.compose(combined) }

            val deserialiser = { serialisedEvent:String ->
                val root = stringToJsonNode(serialisedEvent)
                val migratedRoot = combinedMigrationFunction(root)
                deserialiseFunction!!(migratedRoot)
            }

            return deserialisers + (Pair(className, version) to deserialiser)
        }

        private fun stringToJsonNode(serialisedEvent: String): JsonNode {
            try {
                return objectMapper.readTree(serialisedEvent)
            } catch (ex: IOException) {
                throw UnparseableJsonPayloadException(ex, serialisedEvent)
            }
        }
    }

    infix fun <IP, R, P1> ((IP) -> R).compose(f: (P1) -> IP): (P1) -> R {
        return { p1: P1 -> this(f(p1)) }
    }

    interface Migration {

        fun fromClassName(): String

        fun toClassName(): String

        fun fromVersion(): Int

        fun toVersion(): Int

        fun migrationFunction(): (JsonNode) -> JsonNode
    }

    class FormatMigration(
            private val className: String,
            private val fromVersion: Int,
            private val toVersion: Int,
            private val migrationFunction: (JsonNode) -> JsonNode) : Migration {

        override fun fromClassName(): String {
            return className
        }

        override fun toClassName(): String {
            return className
        }

        override fun fromVersion(): Int {
            return fromVersion
        }

        override fun toVersion(): Int {
            return toVersion
        }

        override fun migrationFunction(): (JsonNode) -> JsonNode {
            return migrationFunction
        }
    }

    class ClassNameMigration(
            private val fromClassName: String,
            private val toClassName: String,
            private val fromVersion: Int,
            private val toVersion: Int) : Migration {

        override fun fromClassName(): String {
            return fromClassName
        }

        override fun toClassName(): String {
            return toClassName
        }

        override fun fromVersion(): Int {
            return fromVersion
        }

        override fun toVersion(): Int {
            return toVersion
        }

        override fun migrationFunction(): (JsonNode) -> JsonNode {
            return { jsonNode -> jsonNode }
        }
    }
}
