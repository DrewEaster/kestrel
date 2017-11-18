package com.dreweaster.ddd.kestrel.infrastructure.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject

object UserLockedMapper : JsonEventMappingConfigurer<UserLocked> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<UserLocked>) {
        configurationFactory.create(UserLocked::class.qualifiedName!!)
            .mappingFunctions({ _ -> jsonObject() }, { _ -> UserLocked})
    }
}