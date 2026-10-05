# Architecture

## Process bootstrap

`RadarBackend.main` parses the CLI options (`RadarBackendCliOptions`) and loads `radar.yml` into a
`RadarConfigHandler` (`RadarConfigHandlerImpl`, a singleton provided by `RadarSingletonFactory`). It then builds a
`BackendProcess` for the subcommand. The only subcommand is `stream` (also the default), which maps to
`KafkaStreamFactory.createSensorStreams()`. Extra CLI arguments after `stream` filter the configured streams by
class-name suffix.

`RadarBackend.run()` starts the process and registers a shutdown hook that calls `shutdown()`.

## Stream layer (master/worker model)

- `StreamMaster` (a `BackendProcess`) owns the lifecycle of a set of `StreamWorker`s. It creates one worker per
  configured `SingleStreamConfig`, by reflection on `streamClass`. It starts and stops the workers on a single
  scheduled executor, and reacts to crashes (`notifyCrashedStream`) and Kafka Streams thread exceptions
  (`restartStream`, retried after `RETRY_TIMEOUT`, 5 minutes).
- `StreamWorker` is the interface every stream implements (`configure`, `start`, `shutdown`,
  `getStreamDefinitions`). `AbstractStreamWorker` provides the common lifecycle, holds the `StreamDefinition`s
  (input/output topic pairs, optionally with an aggregation window and a `GlobalStoreConfig`), and implements
  `StreamsUncaughtExceptionHandler`.
- A concrete stream subclasses `AbstractStreamWorker` and implements `initialize()` (define one or more
  `StreamDefinition`s from `config.properties`) and `createStreams()` (build the `KafkaStreams` instances).
- Each `stream.streams[]` entry in `radar.yml` names a `StreamWorker` class. Its `properties` map is available to
  that worker's `initialize()`.

## Rule engine stream (`org.radarbase.stream.ruleengine`)

`RuleEngineStream` is a just-in-time intervention rule engine built on a Kafka Streams global store. Stream
properties: `input_topic`, `output_topic`, `global_store_name`, `global_store_topic`.

- **Rules.** `InterventionConfig`, `ConditionConfig` and `ActionConfig` (`org.radarbase.config.intervention`) define
  the shape of a rule document. Rules arrive on `global_store_topic`, a CDC-style config topic: the key is Avro
  (`RuleKeySerde`), and the value is Avro with a JSON-encoded `InterventionConfig` inside (`InterventionConfigSerde`).
  `src/main/avro/rule_engine_config_*.avsc` documents both schemas; the serdes embed the same schemas inline, and no
  Avro Gradle plugin is applied.
- **Global store.** `RuleGroupProcessor` maintains a persistent key-value store, `global_store_name`, mapping a scope
  string to a `RuleGroup`. The scopes are `global`, `project.<id>` and `user.<id>` (`ScopePrefix`). A rule that
  changes scope is removed from its old scopes, and a tombstone removes it from all scopes. `RuleGroup.rules` is
  keyed by `RuleKey.toStoreKey()` (a string), because Jackson cannot deserialize a map keyed by `RuleKey`.
- **Evaluation.** `RuleProcessor` consumes `input_topic` as Avro `GenericRecord`s, using the default Avro serde and
  therefore the schema registry. It looks up the `RuleGroup`s for `global`, `project.<projectId>` and
  `user.<userId>` (fields of the record's `ObservationKey`), and evaluates each rule once, even when it is stored
  under several scopes. A rule fires when all of its conditions that apply to the record's scopes hold; conditions
  scoped to other projects or users are ignored, and an evaluation error counts as no match.
  `ConditionFactory` selects a `ConditionEvaluator` by `ExpressionType`.
  `CelConditionEvaluator` compiles and caches CEL (`dev.cel:cel`) programs, with the record exposed as `key` and
  `value` maps (`toCelCompatible()` converts Avro types into plain Kotlin types).
- **Output.** Each matching rule forwards one record per `ActionConfig` whose scope applies to the record, to
  `output_topic`, with a `null` key and the action serialized as JSON (`JsonSerde`).
- `doCleanup()` closes `kafkaStreamsInstances` with a 1 s timeout. `AbstractStreamWorker.closeStreams()` sets
  `streams = null` before calling it, so in practice it closes nothing and `shutdown()` relies on the unbounded
  `KafkaStreams.close()`.
- `AbstractStreamWorker.handle()` runs on the failed stream thread and only returns a response; it never closes the
  client from that thread, because `close()` waits for that same thread to stop. A `StreamsException` gets
  `REPLACE_THREAD`, and the new thread resumes from the last committed offset. Any other error gets
  `SHUTDOWN_APPLICATION`, and the `StreamMaster` is notified from a separate thread.
- `RuleProcessor` skips a record when no `global` rule group is stored.

## Configuration

`RadarBackendConfig` is the root Jackson model of `radar.yml`, loaded by `RadarConfigHandlerImpl` through
`YamlConfigLoader` (radar-commons-server). It covers the server lists (`zookeeper`, `broker`, `schema_registry`,
`rest_proxy`), `stream` (`StreamConfig`: thread counts per priority, commit intervals, Kafka `properties`, and
`streams`), `extras`, and `build_version`. Unknown keys fail to load.

`KafkaProperty` and `getStreamProperties()` (`org.radarbase.util.StreamUtil`) build the per-stream `Properties` for
`KafkaStreams`. Entries in `stream.properties` can be overridden or added through `STREAM_*` environment variables
(`ConfigUtils.withEnvVars`): `STREAM_SASL_JAAS_CONFIG` becomes `sasl.jaas.config`. Use these for credentials; never
put credentials in `radar.yml`.

## Adding a stream

Create a `StreamWorker`, usually by extending `AbstractStreamWorker` as `RuleEngineStream` does, and register it under
`stream.streams` in `radar.yml` with its `class` and the `properties` it requires.
