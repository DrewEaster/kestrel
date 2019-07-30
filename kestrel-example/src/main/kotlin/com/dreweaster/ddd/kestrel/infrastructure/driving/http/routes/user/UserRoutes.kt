package com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes.user

import com.dreweaster.ddd.kestrel.application.*
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserDTO
import com.dreweaster.ddd.kestrel.application.readmodel.user.UserReadModel
import com.dreweaster.ddd.kestrel.domain.aggregates.user.RegisterUser
import com.dreweaster.ddd.kestrel.domain.aggregates.user.User
import com.dreweaster.ddd.kestrel.infrastructure.driving.http.routes.BaseRoutes
import com.github.salomonbrys.kotson.jsonObject
import com.github.salomonbrys.kotson.string
import com.google.gson.JsonObject
import com.google.inject.Inject
import io.ktor.application.*
import io.ktor.http.HttpStatusCode
import io.ktor.response.*
import io.ktor.routing.*

class UserRoutes @Inject constructor(application:Application, domainModel: DomainModel, userReadModel: UserReadModel): BaseRoutes() {

    init {
        application.routing {

            route("/users") {

                get {
                    call.respondWithJson(userReadModel.findAllUsers().map { userToJsonObject(it) })
                }

                post {
                    val request = RegisterUserRequest(call.receiveJson())
                    val user = domainModel.aggregateRootOf(User, request.id)
                    val result = RegisterUser(request.username, request.password) sendTo user

                    when(result) {
                        is SuccessResult -> call.respondWithJson(jsonObject("id" to request.id.value))
                        else -> call.respond(HttpStatusCode.InternalServerError)
                    }
                }

                route("{id}") {
                    get {
                        val user = userReadModel.findUserById(call.parameters["id"]!!)
                        if (user == null) call.respond(HttpStatusCode.NotFound) else call.respondWithJson(userToJsonObject(user))
                    }
                }
            }
        }
    }

    fun userToJsonObject(user: UserDTO) =
        jsonObject(
            "id" to user.id,
            "username" to user.username,
            "password" to user.password,
            "locked" to user.locked
        )
}

data class RegisterUserRequest(val id: AggregateId, val username: String, val password: String) {
    companion object {
        operator fun invoke(requestBody: JsonObject): RegisterUserRequest {
            val username = requestBody["username"].string
            val password = requestBody["password"].string
            return RegisterUserRequest(AggregateId(IdGenerator.randomId()), username, password)
        }
    }
}