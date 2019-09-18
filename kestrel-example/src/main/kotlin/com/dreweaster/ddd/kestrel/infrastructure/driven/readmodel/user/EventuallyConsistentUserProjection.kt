package com.dreweaster.ddd.kestrel.infrastructure.driven.readmodel.user

import com.dreweaster.ddd.kestrel.infrastructure.rdbms.EventuallyConsistentDatabaseProjection

class EventuallyConsistentUserProjection: EventuallyConsistentDatabaseProjection {
//
//    override val update = projection(name = "") {
//
//        subscribe(context = UserContext, edenPolicy = BoundedContextSubscriptionEdenPolicy.BEGINNING_OF_TIME) {
//
//            event<UserRegistered> { e ->
//                statement("INSERT into usr (id, username, password, locked) VALUES ($1, $2, $3, $4)") {
//                    this["$1"] = e.aggregateId.value
//                    this["$2"] = e.rawEvent.username
//                    this["$3"] = e.rawEvent.password
//                    this["$4"] = false
//                }
//            }
//
//            event<UsernameChanged> { e ->
//                statement("UPDATE usr SET username = $1 WHERE id = $2") {
//                    this["$1"] = e.aggregateId.value
//                    this["$2"] = e.rawEvent.username
//                }.expect(1)
//            }
//
//            event<PasswordChanged> { e ->
//                statement("UPDATE usr SET password = $1 WHERE id = $2") {
//                    this["$1"] = e.aggregateId.value
//                    this["$2"] = e.rawEvent.password
//                }.expect(1)
//            }
//
//            event<UserLocked> { e ->
//                statement("UPDATE usr SET locked = $1 WHERE id = $2") {
//                    this["$1"] = e.aggregateId.value
//                    this["$2"] = true
//                }.expect(1)
//            }
//
//            event<UserUnlocked> { e ->
//                statement("UPDATE usr SET locked = $1 WHERE id = $2") {
//                    this["$1"] = e.aggregateId.value
//                    this["$2"] = false
//                }.expect(1)
//            }
//
////            drop {
////
////            }
//        }
//    }
}