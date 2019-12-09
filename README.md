# kestrel
Simple DDD toolkit for Kotlin

TODO
====

* Spring Kofu example
* Complete the reactive refactor
    * Testing different failure scenarios
* General testing
    * HTTP event sources
    * Database backend
    * Projections
* Eventually consistent (async) DB projections (db offset manager)
* Support for dry run command handling (i.e. handle command but don't persist events)
* Utilise Reactor metrics to gather useful metrics
* Support for storing events not tied to aggregates
* Stateful process manager support
* Switch stateless process manager to be similar to stateful one (e.g. explicit domain service dependency)
* Register (stateless) process managers with application rather than start themselves
* Type-safe SQL string builder (maybe fork initially from Ktorm)
* Simple non-event sourced aggregate support
    * Simple command handling (no behaviours)
    * State serialisation (Object -> DB). ORM...
    * Optional event generation
* Kestrel application bootstrap module (e.g. KestrelRdbmsApplication)
* Debezium + Kafka/Kinesis stream support
* Default JSON mapper functionality for v1 of events? Or maybe at least code generation?
* Create CLI toolbelt for auto generation of skeleton code
* Document need for adequate indexes on domain_event table
* Websocket events subscriptions and streams - abstract difference between fetching straight from backend store or via external stream (kafka/kinesis)
    * Transparent leader election to prevent multiple subscriptions receiving duplicates
* Command deduplication - remember and return prior rejections