package com.dreweaster.ddd.kestrel.domain

data class DomainEventTag(val value: String)

interface Persistable

interface DomainEvent: Persistable {
    val tag: DomainEventTag
}

interface DomainCommand