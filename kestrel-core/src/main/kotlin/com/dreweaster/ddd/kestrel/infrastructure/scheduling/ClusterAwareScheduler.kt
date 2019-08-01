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
class ClusterAwareScheduler(private val clusterManager: Cluster): Scheduler {

    private val LOG = LoggerFactory.getLogger(ClusterAwareScheduler::class.java)

    private val jobs = mutableListOf<Disposable>()

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job) {
        val jobScheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor()) // TODO: must dispose of this

        jobs.add(delay(repeatSchedule)
            .publishOn(jobScheduler)
            .subscribeOn(jobScheduler)
            .flatMap { ClusterSingletonJobWrapper(job).execute() }
            .timeout(repeatSchedule.multipliedBy(10)) // TODO: needs to be configurable
            .then(fromCallable { LOG.info("Job execution succeeded: '${job.name}'")})
            .onErrorResume { fromCallable { LOG.error("Job execution failed: '${job.name}'", it) } }
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
        return empty()
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override fun execute(): Mono<Void> {
            return clusterManager.iAmTheLeader()
                .flatMap { iAmTheLeader ->
                    when {
                        iAmTheLeader -> {
                            LOG.debug("Running job '$name' as this instance is leader")
                            wrappedJob.execute()
                        }
                        else -> {
                            LOG.debug("Not running job '$name' as this instance is not leader")
                            empty<Unit>()
                        }
                    }
                }.then()
        }
    }
}