# kestrel
Simple DDD toolkit for Kotlin

TODO
====

* Complete the reactive refactor
* Use Kotlin inline classes for IDs
* Support for dry run command handling (i.e. handle command but don't persist events)
* Support for automatic snapshotting of event sourced entities
* Sort of dependencies on Database module - PostgresOffsetTracker in wrong module?
* Re-enable reporters/probes
* Get rid of event stream terminology where it's not really a stream
* Eventually consistent DB projections (db offset manager)
* Stateful process manager support
* Type-safe SQL string builder
* Kestrel application bootstrap module (e.g. KestrelRdbmsApplication)
* Debezium + Kafka/Kinesis stream support
* Consider dropping implicit migration of event payloads in http event sources - introduce versioning into consumers