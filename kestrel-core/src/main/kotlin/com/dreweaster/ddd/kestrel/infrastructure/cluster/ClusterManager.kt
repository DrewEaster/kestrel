package com.dreweaster.ddd.kestrel.infrastructure.cluster

import java.time.Instant

interface ClusterManager {

    interface Listener {
        fun onElected(at: Instant)
        fun onUnelected(at: Instant)
    }

    fun addListener(listener: Listener)

    suspend fun iAmTheLeader(): Boolean
}

object LocalClusterManager : ClusterManager {

    override suspend fun iAmTheLeader() = true

    override fun addListener(listener: ClusterManager.Listener) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}