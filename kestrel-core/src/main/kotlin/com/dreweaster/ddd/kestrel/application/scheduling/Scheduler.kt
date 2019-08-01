package com.dreweaster.ddd.kestrel.application.scheduling

import reactor.core.publisher.Mono
import java.time.Duration

interface Job {

    val name: String

    fun execute(): Mono<Void>
}

interface Scheduler {

    fun scheduleManyTimes(repeatSchedule: Duration, job: Job)

    // TODO: Terminate all scheduled jobs so that JVM can shutdown as gracefully as possible
    // Should help for hot restart situations in development
    fun shutdown(): Mono<Void>
}