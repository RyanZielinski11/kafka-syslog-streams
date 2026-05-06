# syslog-ng-streams

Configurable Kafka Streams router for Avro-backed syslog events. The app consumes one source topic, evaluates a set of independent rules, and republishes matching records to one or more downstream topics without changing the Avro payload.

## Current capabilities

- reads Avro records from a configured source topic using Schema Registry
- evaluates rules independently, so one event can fan out to multiple topics
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

Each rule is defined with a `rule.<name>.*` prefix.

Required rule properties:

- `rule.<name>.topic`
- `rule.<name>.patterns`

Optional rule properties:

- `rule.<name>.field`
- `rule.<name>.matchType`
- `rule.<name>.mode`
- `rule.<name>.ignoreCase`
- `rule.<name>.enabled`
- `rule.<name>.requireAllPatterns`

Example config lives in [syslog-streams.properties.example](/Users/ryanzielinski/syslog-ng-streams/syslog-streams.properties.example).

## Example behavior

These three rules reproduce the current rough behavior:

- `infoblox`: include records where `rawMessage` contains `netauto_`
- `security`: include records where `rawMessage` contains `sshd`, `LOGIN`, or `AUTH_FAIL`
- `default`: exclude records where `rawMessage` contains `netauto_`

Because rules are evaluated independently, a record can be written to more than one output topic.

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
