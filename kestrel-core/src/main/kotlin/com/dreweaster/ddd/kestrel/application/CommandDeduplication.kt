package com.dreweaster.ddd.kestrel.application

import java.time.Duration
import java.time.Instant

interface CommandDeduplicationStrategy {

    fun isDuplicate(commandId: CommandId): Boolean
}

interface CommandDeduplicationStrategyBuilder {

    fun addEvent(domainEvent: PersistedEvent<*>): CommandDeduplicationStrategyBuilder

    fun build(): CommandDeduplicationStrategy
}

interface CommandDeduplicationStrategyFactory {

    fun newBuilder(): CommandDeduplicationStrategyBuilder
}

class TimeRestrictedCommandDeduplicationStrategy(private val causationIds: Set<CausationId>) : CommandDeduplicationStrategy {

    override fun isDuplicate(commandId: CommandId): Boolean {
        return causationIds.contains(CausationId(commandId.value))
    }

    class Builder(private val barrierDate: Instant) : CommandDeduplicationStrategyBuilder {

        private var causationIds: Set<CausationId> = emptySet()

        override fun addEvent(domainEvent: PersistedEvent<*>): CommandDeduplicationStrategyBuilder {
            if (domainEvent.timestamp.isAfter(barrierDate)) {
                causationIds += domainEvent.causationId
            }
            return this
        }

        override fun build(): CommandDeduplicationStrategy {
            return TimeRestrictedCommandDeduplicationStrategy(causationIds)
        }
    }
}

class TwentyFourHourWindowCommandDeduplicationStrategyFactory : CommandDeduplicationStrategyFactory {

    override fun newBuilder(): CommandDeduplicationStrategyBuilder {
        return TimeRestrictedCommandDeduplicationStrategy.Builder(Instant.now().minus(Duration.ofHours(24)))
    }
}
