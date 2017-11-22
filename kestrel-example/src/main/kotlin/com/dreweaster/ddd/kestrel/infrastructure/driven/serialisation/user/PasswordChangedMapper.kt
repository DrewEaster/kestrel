package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string

object PasswordChangedMapper : JsonEventMappingConfigurer<PasswordChanged> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<PasswordChanged>) {
        configurationFactory.create(PasswordChanged::class.qualifiedName!!)
            .mappingFunctions(
                { event ->
                    jsonObject(
                        "old_password" to event.oldPassword,
                        "password" to event.password
                    )
                },
                { node ->
                    PasswordChanged(
                        oldPassword = node["old_password"].string,
                        password = node["password"].string
                    )
                }
            )
    }
}