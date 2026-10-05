# radar-kafka-stream-apps

Kafka Streams applications for the RADAR-base platform. It currently contains a rule engine for just-in-time
interventions: rules are read from a config topic into a global store, evaluated as
[CEL](https://cel.dev) expressions against records on an input topic, and the actions of matching rules are written
to an output topic.

This project is the successor of the stream framework and rule engine in
[RADAR-Backend](https://github.com/RADAR-base/RADAR-Backend). It keeps that code's packages, class names and
`radar.yml` format.

## Requirements

- JDK 21 (the Gradle wrapper provisions the toolchain)
- A Kafka cluster and a Confluent Schema Registry
- Docker, for the integration tests and the Docker image

## Building and testing

```shell
./gradlew build              # compile, unit tests and ktlint
./gradlew integrationTest2   # integration tests against a Testcontainers Kafka broker (needs Docker)
./gradlew installDist        # start script in build/install/radar-kafka-stream-apps/bin
```

## Configuration

The application reads a YAML file (`radar.yml`; see the one in the repository root):

```yaml
broker:
  - host: radar-kafka-kafka-bootstrap
    port: 9094

schema_registry:
  - host: localhost
    port: 8081
    protocol: http

stream:
  threads_per_priority:
    low: 1
    normal: 2
    high: 4
  properties:          # Kafka Streams properties, applied to every stream
    session.timeout.ms: 20000
  streams:
    - class: org.radarbase.stream.ruleengine.RuleEngineStream
      properties:
        input_topic: questionnaire_response      # Avro records to evaluate
        output_topic: rule_engine_output         # JSON-encoded actions of matching rules
        global_store_name: JIT_RULES_STORE       # name of the global rule store
        global_store_topic: rule_engine_config   # topic with the rule definitions
```

Unknown keys are rejected. Kafka properties can also be set through environment variables with the `STREAM_`
prefix: the rest of the name is lowercased and `_` becomes `.`. Use this for credentials, for example:

```shell
export STREAM_SECURITY_PROTOCOL=SASL_PLAINTEXT
export STREAM_SASL_MECHANISM=SCRAM-SHA-512
export STREAM_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username="..." password="...";'
```

The key and value schemas of the rule config topic are documented in `src/main/avro/rule_engine_config_*.avsc`.

## Running

```shell
./gradlew run --args="-c radar.yml stream"
# or
build/install/radar-kafka-stream-apps/bin/radar-kafka-stream-apps -c radar.yml stream
```

`stream` starts every stream in `stream.streams`. Extra arguments limit this to streams whose class name ends with
one of them, for example `stream RuleEngineStream`.

## Docker

```shell
docker build -t radar-kafka-stream-apps .
docker run --rm \
  -e KAFKA_SCHEMA_REGISTRY=http://schema-registry:8081 \
  -v "$PWD/radar.yml:/etc/radar.yml" \
  radar-kafka-stream-apps radar-kafka-stream-apps -c /etc/radar.yml stream
```

The entrypoint, `radar-kafka-stream-apps-init`, waits until the schema registry in `KAFKA_SCHEMA_REGISTRY` is
reachable and then runs the given command.

## Architecture

See [docs/architecture.md](docs/architecture.md).

## License

Apache License 2.0; see [LICENSE](LICENSE).
