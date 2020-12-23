package com.dreweaster.ddd.kestrel.infrastructure.driving.processmanager.stateless

import com.dreweaster.ddd.kestrel.application.BoundedContextEventSources
import com.dreweaster.ddd.kestrel.application.BoundedContextSubscriptionEdenPolicy.FROM_NOW
import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.application.processmanager.stateless.WarnUserLocked
import com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked
import com.dreweaster.ddd.kestrel.infrastructure.processmanagers.StatelessProcessManagerMaterialiser

class WarnUserLockedMaterialiser(boundedContexts: BoundedContextEventSources): StatelessProcessManagerMaterialiser(boundedContexts) {

    init {
        materialise(WarnUserLocked) {

            subscribe(name = "user-lockouts", context = UserContext, edenPolicy = FROM_NOW) {
                event<UserLocked>()
            }

        }.start()
    }
}