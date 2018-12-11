package com.dreweaster.ddd.kestrel.application.job

import java.time.Duration

interface Job {

    val name: String

    suspend fun execute()
}

interface JobManager {

    fun scheduleManyTimes(repeatSchedule: Duration, job: Job)
}