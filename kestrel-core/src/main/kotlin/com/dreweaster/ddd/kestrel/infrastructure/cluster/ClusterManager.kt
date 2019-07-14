package com.dreweaster.ddd.kestrel.infrastructure.cluster

import io.reactivex.Single
interface ClusterManager {

    fun iAmTheLeader(): Single<Boolean>
}

object LocalClusterManager : ClusterManager {

    override fun iAmTheLeader() = Single.just(true)
}