# syslog-ng-streams

Small Kafka Streams application that reads Avro-encoded syslog events, applies a few content-based filters, and writes matching records to downstream topics.

## What it does

- reads from a single input topic
- routes Infoblox-style events when `rawMessage` contains `netauto_`
- routes security events when `rawMessage` contains `sshd`, `LOGIN`, or `AUTH_FAIL`
- routes all non-`netauto_` events to a default output topic

## Configuration

The app reads configuration from Java system properties first, then environment variables.

| Setting | Default |
| --- | --- |
| `APP_ID` | `syslog-ng-streams` |
| `BOOTSTRAP_SERVERS` | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` |
| `INPUT_TOPIC` | `syslog.raw.avro` |
| `INFOBLOX_TOPIC` | `syslog.infoblox` |
| `SECURITY_TOPIC` | `syslog.security` |
| `DEFAULT_TOPIC` | `syslog.default` |

Example:

```bash
export BOOTSTRAP_SERVERS=broker1:9092
export SCHEMA_REGISTRY_URL=http://schema-registry:8081
export INPUT_TOPIC=syslog_ng.QUAN.syslog.raw.avro
export INFOBLOX_TOPIC=syslog_ng.QUAN.infoblox.test
export SECURITY_TOPIC=syslog_ng.QUAN.filtered.test
export DEFAULT_TOPIC=syslog_ng.QUAN.final.test
```

## Build

```bash
mvn package
```

## Run

```bash
java -jar target/syslog-ng-streams-1.0-SNAPSHOT.jar
```
