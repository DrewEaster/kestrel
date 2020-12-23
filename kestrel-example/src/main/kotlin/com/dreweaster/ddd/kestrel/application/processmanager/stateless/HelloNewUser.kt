package com.dreweaster.ddd.kestrel.application.processmanager.stateless

import com.dreweaster.ddd.kestrel.application.StatelessProcessManager
import com.dreweaster.ddd.kestrel.domain.UserAnnounced
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.fromRunnable

object HelloNewUser: StatelessProcessManager {

    private val LOG = LoggerFactory.getLogger(HelloNewUser::class.java)

    override val behaviour = processManager(name = "hello-new-user") {

        event<UserAnnounced> { event, _ ->
            fromRunnable { LOG.info("Hello ${event.name}!") }
        }
    }
}