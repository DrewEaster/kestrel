package com.dreweaster.ddd.kestrel.infrastructure.job

import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.infrastructure.cluster.ClusterManager
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class ScheduledExecutorServiceJobManager(
        private val clusterManager: ClusterManager,
        private val scheduler: ScheduledExecutorService): JobManager {

    private val LOG = LoggerFactory.getLogger(ScheduledExecutorServiceJobManager::class.java)

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job) {
        LOG.debug("Scheduling job: '${job.name}'")
        // It's okay to block waiting for future result as we're using a dedicated job execution context
        // It's important that we wait for job to complete execution so that it's not rescheduled if the previous invocation hasn't yet completed
        scheduler.scheduleAtFixedRate({
            val threadContext = newSingleThreadContext(job.name)
            try {
                runBlocking(threadContext) {
                    // TODO: Make timeout configurable - defaulting to 10x the repeat schedule
                    withTimeout(repeatSchedule.toMillis() * 10) {
                        ClusterSingletonJobWrapper(job).execute()
                    }
                }
            } catch(ex: Exception) {
                LOG.error("Job execution failed: '${job.name}'", ex)
            } finally {
                try {
                    threadContext.close()
                } catch(ex: Exception) {
                    LOG.error("Failed to close thread context for job: '${job.name}'", ex)
                }
            }
        }, repeatSchedule.toMillis(), repeatSchedule.toMillis(), TimeUnit.MILLISECONDS)
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override suspend fun execute() {
            if (clusterManager.iAmTheLeader()) {
                LOG.debug("Running job '$name' as this instance is leader")
                wrappedJob.execute()

            } else {
                LOG.debug("Not running job '$name' as this instance is not leader")
            }
        }
    }
}