package com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json

import com.dreweaster.ddd.kestrel.application.PersistableMappingContext
import com.dreweaster.ddd.kestrel.application.MappingException
import com.dreweaster.ddd.kestrel.application.PersistableSerialisationResult
import com.dreweaster.ddd.kestrel.application.SerialisationContentType
import com.dreweaster.ddd.kestrel.domain.Persistable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.IOException

interface JsonMapperBuilder<Data : Persistable> {

    fun migrateFormat(migration: ((ObjectNode) -> ObjectNode)): JsonMapperBuilder<Data>

    fun migrateClassName(className: String): JsonMapperBuilder<Data>

    fun mappingFunctions(serialiseFunction: ((Data) ->  ObjectNode), deserialiseFunction: ((ObjectNode) -> Data))
}

interface JsonMapperBuilderFactory<Data : Persistable> {

    fun create(initialClassName: String): JsonMapperBuilder<Data>
}

interface JsonMapper<Data : Persistable> {

    fun configure(factory: JsonMapperBuilderFactory<Data>)
}

class UnparseableJsonPayloadException(cause: Throwable, serialisedPayload: String) : MappingException("Could not parse JSON payload: " + serialisedPayload, cause)

class MissingDeserialiserException(serialisedType: String, serialisedVersion: Int) : MappingException(
        "No deserialiser found for type = '$serialisedType' with = '$serialisedVersion'")

class MissingSerialiserException(type: String) : MappingException("No serialiser found for type = '$type'")

class JsonMappingContext(mappers: List<JsonMapper<Persistable>>) : PersistableMappingContext {

    private val objectMapper: ObjectMapper = ObjectMapper()

    private var deserialisers: Map<Pair<String, Int>, (String) -> Persistable> = emptyMap()
    private var serialisers: Map<String, (Persistable) -> Pair<String, Int>> = emptyMap()

    init {
        init(mappers)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <E : Persistable> deserialise(serialisedPayload: String, serialisedType: String, serialisedVersion: Int): E {
        val deserialiser = deserialisers[Pair(serialisedType, serialisedVersion)] ?: throw MissingDeserialiserException(serialisedType, serialisedVersion)
        return deserialiser(serialisedPayload) as E
    }

    override fun <E : Persistable> serialise(data: E): PersistableSerialisationResult {
        val serialiser = serialisers[data::class.qualifiedName!!] ?: throw MissingSerialiserException(data::class.qualifiedName!!)

        val versionedPayload = serialiser(data)

        return PersistableSerialisationResult(
            versionedPayload.first,
            SerialisationContentType.JSON,
            versionedPayload.second
        )
    }

    private fun init(configurers: List<JsonMapper<Persistable>>) {
        // TODO: Validate no clashes between registered mappers
        // e.g. what if two mappers try to convert to the same class?
        // e.g. what if a com.dreweaster.ddd.jester.infrastructure.driven.eventstore.com.dreweaster.ddd.jester.infrastructure.driven.eventstore.postgres.db.migration in one mapper maps to a class name in another mapper?
        // Such scenarios should be made impossible (at least for v1...)

        val mappingConfigurations = configurers.map {
            val mappingConfiguration = MappingConfiguration<Persistable>()
            it.configure(mappingConfiguration)
            mappingConfiguration
        }

        deserialisers = mappingConfigurations.fold(deserialisers) { acc, mappingConfiguration -> acc + mappingConfiguration.createDeserialisers() }

        serialisers = mappingConfigurations.fold(serialisers) { acc, mappingConfiguration -> acc + mappingConfiguration.createSerialiser() }
    }

    inner class MappingConfiguration<Data : Persistable> : JsonMapperBuilderFactory<Data>, JsonMapperBuilder<Data> {

        private var currentVersion: Int = 0

        private var currentClassName: String? = null

        private var migrations: List<Migration> = emptyList()

        private var serialiseFunction: ((Data) -> ObjectNode)? = null

        private var deserialiseFunction: ((ObjectNode) -> Data)? = null

        override fun migrateFormat(migration: ((ObjectNode) -> ObjectNode)): JsonMapperBuilder<Data> {
            migrations += FormatMigration(currentClassName!!, currentVersion, currentVersion + 1, migration)
            currentVersion += 1
            return this
        }

        override fun migrateClassName(className: String): JsonMapperBuilder<Data> {
            val migration = ClassNameMigration(currentClassName!!, className, currentVersion, currentVersion + 1)
            migrations += migration
            currentClassName = migration.toClassName
            currentVersion = migration.toVersion
            return this
        }

        override fun mappingFunctions(serialiseFunction: ((Data) -> ObjectNode), deserialiseFunction: ((ObjectNode) -> Data)) {
            this.serialiseFunction = serialiseFunction
            this.deserialiseFunction = deserialiseFunction
        }

        override fun create(initialClassName: String): JsonMapperBuilder<Data> {
            currentClassName = initialClassName
            currentVersion = 1
            return this
        }

        @Suppress("UNCHECKED_CAST")
        fun createSerialiser(): Pair<String, (Data) -> Pair<String, Int>> {
            return Pair(currentClassName!!) { data ->
                val serialisedJsonData = serialiseFunction!!(data as Data)
                Pair(objectMapper.writeValueAsString(serialisedJsonData), currentVersion)
            }
        }

        fun createDeserialisers(): Map<Pair<String, Int>, (String) -> Persistable> {
            var deserialisers: Map<Pair<String, Int>, (String) -> Persistable> = emptyMap()

            if (!migrations.isEmpty()) {
                deserialisers = putDeserialisers(migrations, deserialisers)
            }

            // Include the 'current' version deserialiser
            deserialisers += (Pair(currentClassName!!, currentVersion) to { serialisedData ->
                val root = stringToObjectNode(serialisedData)
                deserialiseFunction!!(root)
            })
            return deserialisers
        }

        private fun putDeserialisers(
                migrations: List<Migration>,
                deserialisers: Map<Pair<String, Int>, (String) -> Persistable>): Map<Pair<String, Int>, (String) -> Persistable> {

            return if (migrations.isEmpty()) {
                deserialisers
            } else {
                putDeserialisers(migrations.drop(1), putDeserialiser(migrations, deserialisers))
            }
        }

        private fun putDeserialiser(
                migrations: List<Migration>,
                deserialisers: Map<Pair<String, Int>, (String) -> Persistable>): Map<Pair<String, Int>, (String) -> Persistable> {

            val migration = migrations.first()
            val className = migration.fromClassName
            val version = migration.fromVersion
            val migrationFunctions = migrations.map { it.migrationFunction }
            val combinedMigrationFunction = migrationFunctions.drop(1).fold(migrationFunctions.first()) { combined, f -> f.compose(combined) }

            val deserialiser = { serialisedData:String ->
                val root = stringToObjectNode(serialisedData)
                val migratedRoot = combinedMigrationFunction(root)
                deserialiseFunction!!(migratedRoot)
            }

            return deserialisers + (Pair(className, version) to deserialiser)
        }

        private fun stringToObjectNode(serialisedData: String): ObjectNode {
            try {
                return objectMapper.readTree(serialisedData) as ObjectNode
            } catch (ex: IOException) {
                throw UnparseableJsonPayloadException(ex, serialisedData)
            }
        }

        infix fun <IP, R, P1> ((IP) -> R).compose(f: (P1) -> IP): (P1) -> R {
            return { p1: P1 -> this(f(p1)) }
        }
    }

    interface Migration {
        val fromClassName: String
        val toClassName: String
        val fromVersion: Int
        val toVersion: Int
        val migrationFunction: (ObjectNode) -> ObjectNode
    }

    data class FormatMigration(
             private val className: String,
             override val fromVersion: Int,
             override val toVersion: Int,
             override val migrationFunction: (ObjectNode) -> ObjectNode) : Migration {

        override val fromClassName = className
        override val toClassName = className
    }

    class ClassNameMigration(
            override val fromClassName: String,
            override val toClassName: String,
            override val fromVersion: Int,
            override val toVersion: Int,
            override val migrationFunction: (ObjectNode) -> ObjectNode =  { jsonNode -> jsonNode }) : Migration
}
