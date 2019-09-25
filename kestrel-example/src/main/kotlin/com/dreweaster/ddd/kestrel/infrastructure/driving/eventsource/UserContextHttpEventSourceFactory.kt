package com.dreweaster.ddd.kestrel.infrastructure.driving.eventsource

import com.dreweaster.ddd.kestrel.application.AggregateDataMappingContext
import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.domain.AggregateData
import com.dreweaster.ddd.kestrel.domain.aggregates.user.*
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceFactory

class UserContextHttpEventSourceFactory(private val userContextAggregateDataMappingContext: AggregateDataMappingContext): BoundedContextHttpEventSourceFactory(UserContext) {

    override val deserialisers = eventDeserialisers {

        tag("user-event") {

            event<UserRegistered> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered", version = 1, handler = mappingContextDeserialiser())
            }

            event<PasswordChanged> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged", version = 1, handler = mappingContextDeserialiser())
            }

            event<UsernameChanged> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged", version = 1, handler = mappingContextDeserialiser())
            }

            event<UserLocked> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked", version = 1, handler = mappingContextDeserialiser())
            }

            event<UserUnlocked> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserUnlocked", version = 1, handler = mappingContextDeserialiser())
            }

            event<FailedLoginAttemptsIncremented> {
                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented", version = 1, handler = mappingContextDeserialiser())
            }
        }
    }

    private fun <Data : AggregateData> mappingContextDeserialiser(): (String, String, Int) -> Data = { payload, type, version ->
        userContextAggregateDataMappingContext.deserialise(payload, type, version)
    }
}