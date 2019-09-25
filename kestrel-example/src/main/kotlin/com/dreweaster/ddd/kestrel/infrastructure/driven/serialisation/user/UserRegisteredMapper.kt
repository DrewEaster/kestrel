package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.dreweaster.ddd.kestrel.util.json.string
import com.fasterxml.jackson.databind.node.ObjectNode
object UserRegisteredMapper : JsonMapper<UserRegistered> {

    val serialiser: (UserRegistered) -> ObjectNode = { event ->
        jsonObject(
            "username" to event.username,
            "password" to event.password
        )
    }

    val deserialiser: (ObjectNode) -> UserRegistered = { node ->
        UserRegistered(
            username = node["username"].string,
            password = node["password"].string
        )
    }

    override fun configure(factory: JsonMapperBuilderFactory<UserRegistered>) {
        factory.create(UserRegistered::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}