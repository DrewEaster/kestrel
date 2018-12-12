package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject

object PasswordChangedMapper : JsonEventMappingConfigurer<PasswordChanged> {

    val serialiser: (PasswordChanged) -> JsonObject = { event ->
        jsonObject(
            "old_password" to event.oldPassword,
            "password" to event.password
        )
    }

    val deserialiser: (JsonObject) -> PasswordChanged = { node ->
        PasswordChanged(
            oldPassword = node["old_password"].string,
            password = node["password"].string
        )
    }

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<PasswordChanged>) {
        configurationFactory.create(PasswordChanged::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}