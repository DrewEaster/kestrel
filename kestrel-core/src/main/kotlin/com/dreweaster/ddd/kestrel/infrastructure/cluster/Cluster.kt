package com.dreweaster.ddd.kestrel.infrastructure.cluster

import reactor.core.publisher.Mono

interface Cluster {

    fun iAmTheLeader(): Mono<Boolean>
}

object LocalCluster : Cluster {

    override fun iAmTheLeader() = Mono.just(true)
}