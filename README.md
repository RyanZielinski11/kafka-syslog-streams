# syslog-ng-streams

Configurable Kafka Streams router for Avro-backed syslog events. The app consumes one source topic, evaluates named stages, and republishes matching records to one or more downstream topics without changing the Avro payload.

## Current capabilities

- reads Avro records from a configured source topic using Schema Registry
- evaluates stages independently, so one event can fan out to multiple topics
- supports multi-stage pipelines where one stage reads from another stage
- supports configurable field lookup such as `rawMessage`
- supports `contains` and `regex` matching
- supports positive routing with `mode=include`
- supports negative routing with `mode=exclude`
- supports per-rule case sensitivity and per-rule target topics
- supports optional `requireAllPatterns=true` when a rule should require every pattern instead of any pattern

## Config model

The app reads a Java properties file. By default it looks for `syslog-streams.properties`, or you can pass a path as the first argument.

Required top-level properties:

- `bootstrap.servers`
- `schema.registry.url`
- `source.topic`

Optional top-level properties:

- `application.id`
- `default.match.field`
- `default.match.type`
- `default.ignore.case`
- `log.incoming.records`

Each stage is defined with a `stage.<name>.*` prefix.

Required stage properties:

- `stage.<name>.topic`
- `stage.<name>.patterns`

Optional stage properties:

- `stage.<name>.input`
- `stage.<name>.field`
- `stage.<name>.matchType`
- `stage.<name>.mode`
- `stage.<name>.ignoreCase`
- `stage.<name>.enabled`
- `stage.<name>.requireAllPatterns`

`stage.<name>.input` defaults to `source`. Set it to another stage name to build a chained pipeline.

Legacy `rule.<name>.*` entries are still supported and behave like source-based stages.

Example config lives in [syslog-streams.properties.example](/Users/ryanzielinski/syslog-ng-streams/syslog-streams.properties.example).

## Example behavior

These stages reproduce the current KSQL-style behavior:

- `infoblox`: include records where `rawMessage` contains `netauto_`
- `security`: include records where `rawMessage` contains `sshd`, `LOGIN`, or `AUTH_FAIL`
- `default`: exclude records where `rawMessage` contains `netauto_`
- `sdwan.initial`: include records matching the configured device names
- `sdwan.bdp`: read from `sdwan.initial` and include BDP-related patterns

Because stages are evaluated independently, a record can be written to more than one output topic. Chained stages let one filtered branch feed another, which is the piece needed for flows like `source -> sdwan.initial -> sdwan.bdp`.

## Build

```bash
mvn package
```

## Run

```bash
cp syslog-streams.properties.example syslog-streams.properties
java -jar target/syslog-ng-streams-1.0-SNAPSHOT.jar syslog-streams.properties
```

## Notes

This version is a configurable routing engine, not yet a full transformation engine. It is structured so we can add mutation and redaction steps later, but today it preserves the incoming Avro record and only filters/routes it.
