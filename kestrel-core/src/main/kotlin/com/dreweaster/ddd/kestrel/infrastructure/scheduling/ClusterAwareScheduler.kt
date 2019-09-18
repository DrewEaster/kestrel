package com.dreweaster.ddd.kestrel.infrastructure.scheduling

import com.dreweaster.ddd.kestrel.application.scheduling.Job
import com.dreweaster.ddd.kestrel.application.scheduling.Scheduler
import com.dreweaster.ddd.kestrel.infrastructure.cluster.Cluster
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import java.time.Duration
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.*
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executors

/**
 * TODO: Need to implement shutdown of jobs
 */
class ClusterAwareScheduler(private val cluster: Cluster): Scheduler {

    private val LOG = LoggerFactory.getLogger(ClusterAwareScheduler::class.java)

    private val jobs = mutableListOf<Disposable>()

    private val jobSchedulers = mutableListOf<reactor.core.scheduler.Scheduler>()

    // TODO: Could introduce some form of exponential back off strategy for persistently failing jobs
    override fun scheduleManyTimes(repeatSchedule: Duration, timeout: Duration, job: Job) {
        val jobScheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor())
        jobSchedulers.add(jobScheduler)

        jobs.add(Mono.from(ClusterSingletonJobWrapper(job).execute())
            .publishOn(jobScheduler)
            .subscribeOn(jobScheduler)
            .timeout(timeout)
            .flatMap { rescheduleImmediately ->
                LOG.info("Job execution succeeded : '${job.name}'")
                Mono.delay(if(rescheduleImmediately) Duration.ZERO else repeatSchedule) }
            .onErrorResume {
                LOG.error("Job execution failed: '${job.name}'", it)
                Mono.delay(repeatSchedule)
            }
            .repeat()
            .subscribe(
                { },
                { error -> LOG.error("Job scheduling unexpectedly ended: '${job.name}'", error) },
                { LOG.error("Job scheduling unexpectedly ended: '${job.name}'") }
            )
        )
    }

    override fun shutdown(): Mono<Void> {
        // TODO: IMPLEMENT PROPERLY
        jobs.forEach { it.dispose() }
        jobSchedulers.forEach { it.dispose() }
        return empty()
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override fun execute(): Mono<Boolean> {
            return cluster.iAmTheLeader()
                .flatMap { iAmTheLeader ->
                    when {
                        iAmTheLeader -> {
                            LOG.debug("Running job '$name' as this instance is leader")
                            wrappedJob.execute()
                        }
                        else -> {
                            LOG.debug("Not running job '$name' as this instance is not leader")
                            Mono.just(false)
                        }
                    }
                }
        }
    }
}