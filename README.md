# kestrel
Simple DDD toolkit for Kotlin

TODO
====

* Implement example using Micronaut framework
    * Good error handling
* Complete the reactive refactor
    * Testing different failure scenarios
* General testing
    * HTTP event sources
    * Database backend
    * Projections
* Eventually consistent (async) DB projections (db offset manager)
* Use Kotlin inline classes for IDs
* Support for dry run command handling (i.e. handle command but don't persist events)
* Support for automatic snapshotting of event sourced entities (with JSON payload mapper functionality)
* Utilise Reactor metrics to gather useful metrics 
* Simple non-event sourced aggregate support
    * Simple command handling (no behaviours)
    * State serialisation (Object -> DB). ORM...
    * Optional event generation
* Stateful process manager support
* Type-safe SQL string builder
* Kestrel application bootstrap module (e.g. KestrelRdbmsApplication)
* Debezium + Kafka/Kinesis stream support
* Switch stateless process manager to be similar to stateful one (e.g. explicit domain service dependency)
* Register (stateless) process managers with application rather than start themselves
* Default JSON mapper functionality for v1 of events? Or maybe at least code generation?
* Create CLI toolbelt for auto generation of skeleton code
* Document need for adequate indexes on domain_event table
* Websocket events subscriptions and streams - abstract difference between fetching straight from backend store or via external stream (kafka/kinesis)
    * Transparent leader election to prevent multiple subscriptions receiving duplicates