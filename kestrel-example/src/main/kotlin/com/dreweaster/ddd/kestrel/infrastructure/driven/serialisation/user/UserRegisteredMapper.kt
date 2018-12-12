package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject

object UserRegisteredMapper : JsonEventMappingConfigurer<UserRegistered> {

    val serialiser: (UserRegistered) -> JsonObject = { event ->
        jsonObject(
            "username" to event.username,
            "password" to event.password
        )
    }

    val deserialiser: (JsonObject) -> UserRegistered = { node ->
        UserRegistered(
            username = node["username"].string,
            password = node["password"].string
        )
    }

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<UserRegistered>) {
        configurationFactory.create(UserRegistered::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}