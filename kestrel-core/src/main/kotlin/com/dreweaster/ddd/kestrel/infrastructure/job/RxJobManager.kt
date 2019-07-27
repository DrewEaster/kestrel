package com.dreweaster.ddd.kestrel.infrastructure.job

import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.infrastructure.cluster.ClusterManager
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
class RxJobManager(private val clusterManager: ClusterManager): JobManager {

    private val LOG = LoggerFactory.getLogger(RxJobManager::class.java)

    private val jobs = mutableListOf<Disposable>()

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job) {
        val jobScheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor()) // TODO: must dispose of this

        jobs.add(delay(Duration.ofMillis(1000))
            .publishOn(jobScheduler)
            .subscribeOn(jobScheduler)
            .flatMap { ClusterSingletonJobWrapper(job).execute() }
            .timeout(Duration.ofMillis(10000))
            .onErrorResume { fromCallable { LOG.error("Job execution failed: '${job.name}'", it) } }
            .repeat()
            .subscribe(
                { _ -> LOG.info("Job executed successfully: '${job.name}'") },
                { error -> LOG.error("Job scheduling unexpectedly completed: '${job.name}'", error) },
                { LOG.error("Job scheduling unexpectedly completed: '${job.name}'") }
            )
        )
    }

    override fun shutdown(): Mono<Unit> {
        // TODO: IMPLEMENT PROPERLY
        jobs.forEach { it.dispose() }
        return empty()
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override fun execute(): Mono<Unit> {
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
                }.single()
        }
    }
}