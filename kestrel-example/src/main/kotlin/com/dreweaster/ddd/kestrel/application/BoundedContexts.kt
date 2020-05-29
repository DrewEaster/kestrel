package com.dreweaster.ddd.kestrel.application

object BoundedContexts {
    object UserContext: BoundedContextName { override val name = "users" }
}