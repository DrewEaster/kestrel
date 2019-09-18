# kestrel
Simple DDD toolkit for Kotlin

TODO
====

* Complete the reactive refactor
    * Testing different failure scenarios
* Improve error handling example HTTP handlers
* General testing
    * HTTP event sources
    * Database backend
    * Projections
* Eventually consistent (async) DB projections (db offset manager)
* Use Kotlin inline classes for IDs
* Support for dry run command handling (i.e. handle command but don't persist events)
* Support for automatic snapshotting of event sourced entities (with JSON payload mapper functionality)
* Utilise Reactor metrics to gather useful metrics 
* Stateful process manager support
* Type-safe SQL string builder
* Kestrel application bootstrap module (e.g. KestrelRdbmsApplication)
* Debezium + Kafka/Kinesis stream support
* Consider dropping implicit migration of event payloads in http event sources - introduce versioning into consumers
* Switch stateless process manager to be similar to stateful one (e.g. explicit domain service dependency)
* Register (stateless) process managers with application rather than start themselves
* Default JSON mapper functionality for v1 of events? Or maybe at least code generation?
* Create CLI toolbelt for auto generation of skeleton code
* Document need for adequate indexes on domain_event table
* Websocket events subscriptions and streams - abstract difference between fetching straight from backend store or via external stream (kafka/kinesis)