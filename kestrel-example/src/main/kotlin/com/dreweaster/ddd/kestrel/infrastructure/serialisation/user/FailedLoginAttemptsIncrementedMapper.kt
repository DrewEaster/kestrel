package com.dreweaster.ddd.kestrel.infrastructure.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurationFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonEventMappingConfigurer
import com.github.salomonbrys.kotson.jsonObject

object FailedLoginAttemptsIncrementedMapper : JsonEventMappingConfigurer<FailedLoginAttemptsIncremented> {

    override fun configure(configurationFactory: JsonEventMappingConfigurationFactory<FailedLoginAttemptsIncremented>) {
        configurationFactory.create(FailedLoginAttemptsIncremented::class.qualifiedName!!)
            .mappingFunctions({ _ -> jsonObject() }, { _ -> FailedLoginAttemptsIncremented })
    }
}