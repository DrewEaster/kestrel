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
* Switch stateless process manager to be similar to stateful one
* Register process managers with application rather than start themselves
* Default JSON mapper functionality for v1 of events
* Create CLI toolbelt for auto generation of skeleton code