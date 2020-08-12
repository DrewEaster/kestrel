package com.dreweaster.ddd.kestrel.application.processmanager.stateless

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy.BEGINNING_OF_TIME
import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.fromRunnable

class WarnUserLocked constructor(boundedContexts: BoundedContextEventSources): StatelessProcessManager(boundedContexts) {

    private val LOG = LoggerFactory.getLogger(WarnUserLocked::class.java)

    init {
        processManager(name = "warn-user-locked") {

            subscribe(name = "user-lockouts", context = UserContext, edenPolicy = BEGINNING_OF_TIME) {

                event<UserLocked> { _, metadata ->
                    fromRunnable { LOG.warn("User ${metadata.aggregateId} was locked!") }
                    //Mono.error(RuntimeException("oh dear :("))
                }
            }
        }.start()
    }
}