package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject

object UsernameChangedMapper : JsonEventMappingConfigurer<UsernameChanged> {

    val serialiser: (UsernameChanged) -> JsonObject = { event ->
        jsonObject(
            "username" to event.username
        )
    }

    val deserialiser: (JsonObject) -> UsernameChanged = { node ->
        UsernameChanged(
            username = node["username"].string
        )
    }

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<UsernameChanged>) {
        configurationFactory.create(UsernameChanged::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}