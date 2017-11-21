package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string

object UserRegisteredMapper : JsonEventMappingConfigurer<UserRegistered> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<UserRegistered>) {
        configurationFactory.create(UserRegistered::class.qualifiedName!!)
            .mappingFunctions(
                { event ->
                    jsonObject(
                        "username" to event.username,
                        "password" to event.password
                    )
                },
                { node ->
                    UserRegistered(
                        username = node["username"].string,
                        password = node["password"].string
                    )
                }
            )
    }
}