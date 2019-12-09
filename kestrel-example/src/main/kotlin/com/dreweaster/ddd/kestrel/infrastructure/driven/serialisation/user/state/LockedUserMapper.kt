package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.state

import com.dreweaster.ddd.kestrel.domain.aggregates.user.ActiveUser
import com.dreweaster.ddd.kestrel.domain.aggregates.user.LockedUser
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.dreweaster.ddd.kestrel.util.json.string
import com.fasterxml.jackson.databind.node.ObjectNode

object LockedUserMapper : JsonMapper<LockedUser> {

    val serialiser: (LockedUser) -> ObjectNode = { event ->
        jsonObject(
            "username" to event.username,
            "password" to event.password
        )
    }

    val deserialiser: (ObjectNode) -> LockedUser = { node ->
        LockedUser(
            username = node["username"].string,
            password = node["password"].string
        )
    }

    override fun configure(factory: JsonMapperBuilderFactory<LockedUser>) {
        factory.create(ActiveUser::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}