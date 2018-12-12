package com.dreweaster.ddd.kestrel.application.consumers

import com.dreweaster.ddd.kestrel.application.eventstream.EventStreamSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.application.consumers.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.eventstream.*
import com.google.inject.Inject
import com.google.inject.Singleton
import org.slf4j.LoggerFactory

@Singleton
class HelloNewUserTwo @Inject constructor(boundedContexts: BoundedContextEventStreamSources) {

    private val LOG = LoggerFactory.getLogger(HelloNewUser::class.java)

    private val eventStreamConfiguration = EventStreamSubscriberConfiguration(name = "hello-new-user", edenPolicy = FROM_NOW)

    private val onUserRegistered: suspend (UserRegistered, EventMetadata) -> Unit = { event, _ ->
        LOG.info("Hello ${event.username}!")
    }

    init {
        boundedContexts[UserContext]?.subscribe(BoundedContextEventStreamSource.EventHandlersBuilder()
                .withHandler(UserRegistered::class, onUserRegistered)
                .build(), eventStreamConfiguration)
    }
}