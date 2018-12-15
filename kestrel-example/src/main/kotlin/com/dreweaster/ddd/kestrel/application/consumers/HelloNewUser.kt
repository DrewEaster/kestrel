package com.dreweaster.ddd.kestrel.application.consumers

import com.dreweaster.ddd.kestrel.application.eventstream.BoundedContextEventStreamSources
import com.dreweaster.ddd.kestrel.application.eventstream.EventStreamSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.application.consumers.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.eventstream.StatelessEventConsumer
import com.google.inject.Inject
import com.google.inject.Singleton
import org.slf4j.LoggerFactory

@Singleton
class HelloNewUser @Inject constructor(boundedContexts: BoundedContextEventStreamSources): StatelessEventConsumer(boundedContexts) {

    private val LOG = LoggerFactory.getLogger(HelloNewUser::class.java)

    init {
        consumer {

            subscribe(context = UserContext, subscriptionName = "hello-new-user", edenPolicy = FROM_NOW) {

                event<UserRegistered> { event, _ ->
                    LOG.info("Hello ${event.username}!")
                }
            }
        }.start()
    }
}