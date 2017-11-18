package com.dreweaster.ddd.kestrel.infrastructure.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string

object UsernameChangedMapper : JsonEventMappingConfigurer<UsernameChanged> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<UsernameChanged>) {
        configurationFactory.create(UsernameChanged::class.qualifiedName!!)
            .mappingFunctions(
                { event ->
                    jsonObject(
                        "username" to event.username
                    )
                },
                { node ->
                    UsernameChanged(
                        username = node["username"].string
                    )
                }
            )
    }
}