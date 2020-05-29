package com.dreweaster.ddd.kestrel.infrastructure.driving.eventsource

import com.dreweaster.ddd.kestrel.application.BoundedContexts.UserContext
import com.dreweaster.ddd.kestrel.domain.DomainEvent
import com.dreweaster.ddd.kestrel.domain.DomainEventTag
import com.dreweaster.ddd.kestrel.domain.Persistable
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMapper
import com.dreweaster.ddd.kestrel.infrastructure.driven.backend.mapper.json.JsonMappingContext
import com.dreweaster.ddd.kestrel.infrastructure.http.eventsource.consumer.BoundedContextHttpEventSourceFactory
import kotlin.reflect.KClass

interface KestrelExampleJsonEventMapper<T: DomainEvent> {

    companion object {
        inline operator fun <reified T: DomainEvent> invoke(t: DomainEventTag, m: JsonMapper<T>): KestrelExampleJsonEventMapper<T> {
            return object: KestrelExampleJsonEventMapper<T> {
                override val eventClass = T::class
                override val eventTag = t
                override val mapper = m
            }
        }
    }

    val eventClass: KClass<T>
    val eventTag: DomainEventTag
    val mapper: JsonMapper<T>
}

class UserContextHttpEventSourceFactory(
        jsonMappers: List<KestrelExampleJsonEventMapper<*>>,
        private val jsonMappingContext: JsonMappingContext): BoundedContextHttpEventSourceFactory(UserContext) {

        private val mappersByTag = jsonMappers.fold(emptyMap<DomainEventTag, List<KestrelExampleJsonEventMapper<*>>>()) { acc, mapper ->
            val mappers = acc[mapper.eventTag] ?: emptyList()
            acc + (mapper.eventTag to mappers + mapper)
        }

    override val deserialisers = eventDeserialisers {

        mappersByTag.forEach { domainEventTag ->

            tag(domainEventTag.key.value) {

                domainEventTag.value.forEach { m ->

                    event(m.eventClass) {

                        jsonMappingContext.historyFor(m.eventClass.qualifiedName!!).forEach { metadata ->

                            deserialiser(
                                type = metadata.type,
                                version = metadata.version,
                                handler = mappingContextDeserialiser()
                            )
                        }
                    }
                }
            }
        }

//        tag("user-event") {
//
//            listOf("").forEach {
//                event(eventClass = UserRegistered::class) {
//                    listOf("").forEach {
//                        deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered", version = 1, handler = mappingContextDeserialiser())
//                    }
//                }
//            }
//
//
//            event<UserRegistered> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserRegistered", version = 1, handler = mappingContextDeserialiser())
//            }
//
//            event<PasswordChanged> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.PasswordChanged", version = 1, handler = mappingContextDeserialiser())
//            }
//
//            event<UsernameChanged> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UsernameChanged", version = 1, handler = mappingContextDeserialiser())
//            }
//
//            event<UserLocked> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserLocked", version = 1, handler = mappingContextDeserialiser())
//            }
//
//            event<UserUnlocked> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.UserUnlocked", version = 1, handler = mappingContextDeserialiser())
//            }
//
//            event<FailedLoginAttemptsIncremented> {
//                deserialiser(type = "com.dreweaster.ddd.kestrel.domain.aggregates.user.FailedLoginAttemptsIncremented", version = 1, handler = mappingContextDeserialiser())
//            }
//        }
    }

    private fun <Data : Persistable> mappingContextDeserialiser(): (String, String, Int) -> Data = { payload, type, version ->
        jsonMappingContext.deserialise(payload, type, version)
    }
}