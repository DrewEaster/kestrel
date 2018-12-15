package com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes

import com.dreweaster.ddd.kestrel.application.AggregateRoot
import com.dreweaster.ddd.kestrel.application.CommandHandlingResult
import com.dreweaster.ddd.kestrel.domain.DomainCommand
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.ktor.application.ApplicationCall
import io.ktor.http.ContentType
import io.ktor.response.respond
import io.ktor.response.respondText
import java.io.InputStreamReader

abstract class BaseRoutes {

    companion object {
        val gson = Gson()
        val jsonParser = JsonParser()
    }

    suspend fun ApplicationCall.respondWithJson(obj: Any) = respondText(gson.toJson(obj), ContentType.Application.Json)

    suspend fun ApplicationCall.receiveJson() = jsonParser.parse(InputStreamReader(request.receiveContent().inputStream())) as JsonObject

    // Define extension to Int
    infix suspend fun <C: DomainCommand, E: DomainEvent> C.sendTo(aggregateRoot: AggregateRoot<C, E>): CommandHandlingResult<E> = aggregateRoot.handleCommand(this)
}