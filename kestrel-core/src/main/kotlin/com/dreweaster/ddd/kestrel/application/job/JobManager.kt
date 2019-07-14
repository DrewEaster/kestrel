package com.dreweaster.ddd.kestrel.application.job

import io.reactivex.Completable
import java.time.Duration

interface Job {

    val name: String

    fun execute(): Completable
}

interface JobManager {

    fun scheduleManyTimes(repeatSchedule: Duration, job: Job)
}