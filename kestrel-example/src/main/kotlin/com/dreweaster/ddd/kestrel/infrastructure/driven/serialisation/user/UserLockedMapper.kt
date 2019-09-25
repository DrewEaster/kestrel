package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.jsonObject

object UserLockedMapper : JsonMapper<UserLocked> {

    override fun configure(factory: JsonMapperBuilderFactory<UserLocked>) {
        factory.create(UserLocked::class.qualifiedName!!)
            .mappingFunctions({ _ -> jsonObject() }, { _ -> UserLocked})
    }
}