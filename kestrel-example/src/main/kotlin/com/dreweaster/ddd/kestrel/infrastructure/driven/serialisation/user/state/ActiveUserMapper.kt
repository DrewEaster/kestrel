package com.dreweaster.ddd.kestrel.infrastructure.driven.serialisation.user.state

import com.dreweaster.ddd.kestrel.domain.aggregates.user.ActiveUser
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapperBuilderFactory
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.util.json.int
import com.dreweaster.ddd.kestrel.util.json.jsonObject
import com.dreweaster.ddd.kestrel.util.json.string
import com.fasterxml.jackson.databind.node.ObjectNode

object ActiveUserMapper : JsonMapper<ActiveUser> {

    val serialiser: (ActiveUser) -> ObjectNode = { event ->
        jsonObject(
            "username" to event.username,
            "password" to event.password,
            "failed_login_attempts" to event.failedLoginAttempts
        )
    }

    val deserialiser: (ObjectNode) -> ActiveUser = { node ->
        ActiveUser(
            username = node["username"].string,
            password = node["password"].string,
            failedLoginAttempts = node["failed_login_attempts"].int
        )
    }

    override fun configure(factory: JsonMapperBuilderFactory<ActiveUser>) {
        factory.create(ActiveUser::class.qualifiedName!!)
            .mappingFunctions(serialiser, deserialiser)
    }
}