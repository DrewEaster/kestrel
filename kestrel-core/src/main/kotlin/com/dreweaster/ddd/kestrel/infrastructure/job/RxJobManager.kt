package com.dreweaster.ddd.kestrel.infrastructure.job

import com.dreweaster.ddd.kestrel.application.job.Job
import com.dreweaster.ddd.kestrel.application.job.JobManager
import com.dreweaster.ddd.kestrel.infrastructure.cluster.ClusterManager
import io.reactivex.Completable
import io.reactivex.Completable.*
import org.slf4j.LoggerFactory
import java.time.Duration
import io.reactivex.Observable
import io.reactivex.rxkotlin.subscribeBy
import io.reactivex.schedulers.Schedulers
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.*

class RxJobManager(private val clusterManager: ClusterManager): JobManager {

    private val LOG = LoggerFactory.getLogger(RxJobManager::class.java)

    override fun scheduleManyTimes(repeatSchedule: Duration, job: Job) {
        val jobScheduler = Schedulers.from(Executors.newSingleThreadExecutor())
        LOG.debug("Scheduling job: '${job.name}'")
        Observable
            .timer(repeatSchedule.toMillis(), MILLISECONDS)
            .subscribeOn(jobScheduler)
            .observeOn(jobScheduler)
            .flatMapCompletable { ClusterSingletonJobWrapper(job).execute() }
            .timeout(repeatSchedule.toMillis() * 10, MILLISECONDS)
            .onErrorResumeNext { fromCallable { LOG.error("Job execution failed: '${job.name}'", it) } }
            .repeat()
            .subscribeBy(
                onComplete = { LOG.error("Job scheduling unexpectedly completed: '${job.name}'") },
                onError = { LOG.error("Job scheduling unexpectedly completed: '${job.name}'", it) }
            )
    }

    inner class ClusterSingletonJobWrapper(private val wrappedJob: Job) : Job {
        override val name = wrappedJob.name

        override fun execute(): Completable {
            return clusterManager.iAmTheLeader()
                .flatMapCompletable { iAmTheLeader ->
                    when {
                        iAmTheLeader -> {
                            LOG.debug("Running job '$name' as this instance is leader")
                            wrappedJob.execute()
                        }
                        else -> {
                            LOG.debug("Not running job '$name' as this instance is not leader")
                            complete()
                        }
                    }
                }
        }
    }
}