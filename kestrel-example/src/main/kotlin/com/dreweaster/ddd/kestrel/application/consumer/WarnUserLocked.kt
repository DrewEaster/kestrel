package com.dreweaster.ddd.kestrel.application.consumer

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.fromRunnable

class WarnUserLocked constructor(boundedContexts: BoundedContextEventSources): StatelessProcessManager(boundedContexts) {

    private val LOG = LoggerFactory.getLogger(WarnUserLocked::class.java)

    init {
        processManager(name = "warn-user-locked") {

            subscribe(context = UserContext, edenPolicy = FROM_NOW) {

                event<UserLocked> { _, metadata ->
                    fromRunnable { LOG.warn("User ${metadata.aggregateId} was locked!") }
                }
            }
        }.start()
    }
}