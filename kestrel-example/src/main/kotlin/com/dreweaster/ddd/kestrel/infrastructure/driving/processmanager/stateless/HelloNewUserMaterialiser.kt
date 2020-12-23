package com.dreweaster.ddd.kestrel.infrastructure.driving.processmanager.stateless

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.processmanager.stateless.HelloNewUser
import com.dreweaster.ddd.kestrel.domain.UserAnnounced
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered
import com.dreweaster.ddd.kestrel.infrastructure.processmanagers.StatelessProcessManagerMaterialiser
import reactor.core.publisher.Mono

class HelloNewUserMaterialiser(boundedContexts: BoundedContextEventSources): StatelessProcessManagerMaterialiser(boundedContexts) {

    init {
        materialise(HelloNewUser) {

            subscribe(name = "user-registrations", context = UserContext, edenPolicy = FROM_NOW) {
                event<UserRegistered> { evt, metadata -> Mono.just(UserAnnounced(name = evt.username) to metadata) }
            }

        }.start()
    }
}