package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.events

import com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.jsonObject

object FailedLoginAttemptsIncrementedMapper : JsonMapper<FailedLoginAttemptsIncremented> {

    override fun configure(factory: JsonMapperBuilderFactory<FailedLoginAttemptsIncremented>) {
        factory.create(FailedLoginAttemptsIncremented::class.qualifiedName!!)
            .mappingFunctions({ _ -> jsonObject() }, { _ -> FailedLoginAttemptsIncremented })
    }
}