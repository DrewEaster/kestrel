package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user

import com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.dreweaster.ddd.kestrel.util.json.string
import com.fasterxml.jackson.databind.node.ObjectNode

object UsernameChangedMapper : JsonMapper<UsernameChanged> {

    val serialiser: (UsernameChanged) -> ObjectNode = { event ->
        jsonObject(
            "username" to event.username
        )
    }

    val deserialiser: (ObjectNode) -> UsernameChanged = { node ->
        UsernameChanged(
            username = node["username"].string
        )
    }

    override fun configure(factory: JsonMapperBuilderFactory<UsernameChanged>) {
        factory.create(UsernameChanged::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}