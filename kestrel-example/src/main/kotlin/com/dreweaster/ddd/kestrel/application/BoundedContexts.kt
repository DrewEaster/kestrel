package com.dreweaster.ddd.kestrel.application

import com.dreweaster.ddd.kestrel.application.BoundedContextName

object BoundedContexts {
    object UserContext: BoundedContextName { override val name = "users" }
}