package com.dreweaster.ddd.kestrel.infrastructure.cluster

import reactor.core.publisher.Mono

interface ClusterManager {

    fun iAmTheLeader(): Mono<Boolean>
}

object LocalClusterManager : ClusterManager {

    override fun iAmTheLeader() = Mono.just(true)
}