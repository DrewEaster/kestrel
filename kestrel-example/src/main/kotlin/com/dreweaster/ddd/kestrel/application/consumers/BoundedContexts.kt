package com.dreweaster.ddd.kestrel.application.consumers

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextName

object BoundedContexts {
    object UserContext: BoundedContextName { override val name = "users" }
}