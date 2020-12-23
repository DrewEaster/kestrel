package com.dreweaster.ddd.kestrel.application.processmanager.stateless

import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.fromRunnable

object WarnUserLocked : StatelessProcessManager {

    private val LOG = LoggerFactory.getLogger(WarnUserLocked::class.java)

    override val behaviour = processManager(name = "warn-user-locked") {

        event<UserLocked> { _, metadata ->
            fromRunnable { LOG.warn("User ${metadata.aggregateId} was locked!") }
        }
    }
}