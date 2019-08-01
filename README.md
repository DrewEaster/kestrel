# kestrel
Simple DDD toolkit for Kotlin

TODO
====

* Complete the reactive refactor
    * Switch all Mono<Void> to Mono<Void>
    * Introduce consumer offset fix from 0.1.11
    * Use r2dbc-pool for connection pooling
* Make timeouts configurable per job
* Use Kotlin inline classes for IDs
* Support for dry run command handling (i.e. handle command but don't persist events)
* Support for automatic snapshotting of event sourced entities
* Sort out dependencies on Database module - PostgresOffsetTracker in wrong module?
* Re-enable reporters/probes
* Get rid of event stream terminology where it's not really a stream
* Eventually consistent (async) DB projections (db offset manager)
* Stateful process manager support
* Type-safe SQL string builder
* Kestrel application bootstrap module (e.g. KestrelRdbmsApplication)
* Debezium + Kafka/Kinesis stream support
* Consider dropping implicit migration of event payloads in http event sources - introduce versioning into consumers
* Switch stateless process manager to be similar to stateful one
* Register process managers with application rather than start themselves
* Default JSON mapper functionality for v1 of events
* Create CLI toolbelt for auto generation of skeleton code
* Document need for adequate indexes on domain_event table
* Websocket events subscriptions and streams - abstract difference between fetching straight from backend store or via external stream (kafka/kinesis)