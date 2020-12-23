package com.dreweaster.ddd.kestrel.domain

data class UserAnnounced(val name: String): DomainEvent {
    override val tag = DomainEventTag("user-event")
}