package com.dreweaster.ddd.kestrel.application.scheduling

import reactor.core.publisher.Mono
import java.time.Duration

interface Job {

    val name: String

    /**
     * @return whether the job should be immediately rescheduled on completion or whether it should wait delay for
     *         its default scheduling cycle before its next execution
     */
    fun execute(): Mono<Boolean>
}

interface Scheduler {

    fun scheduleManyTimes(repeatSchedule: Duration, timeout: Duration, job: Job)

    // TODO: Terminate all scheduled jobs so that JVM can shutdown as gracefully as possible
    // Should help for hot restart situations in development
    fun shutdown(): Mono<Void>
}