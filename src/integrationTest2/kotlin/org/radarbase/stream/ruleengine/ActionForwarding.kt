package org.radarbase.stream.ruleengine

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.ScopePrefix
import org.radarbase.stream.ruleengine.harness.KafkaBroker
import org.radarbase.stream.ruleengine.harness.KafkaBrokerExtension
import org.radarbase.stream.ruleengine.harness.KafkaTopics
import org.radarbase.stream.ruleengine.harness.OutputTopicConsumer
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.RuleEngineWorkers
import org.radarbase.stream.ruleengine.harness.RuleFixtures.rule
import org.radarbase.stream.ruleengine.harness.StateStoreQueries
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import org.radarbase.stream.ruleengine.harness.StreamTeardown
import org.radarbase.stream.ruleengine.harness.Topics
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.pebble.Pebble2HeartRate
import java.nio.file.Path
import java.time.Duration
import java.util.Properties

@ExtendWith(KafkaBrokerExtension::class)
class ActionForwarding {

    private val workers = mutableListOf<RuleEngineStream>()
    private val mapper = jacksonObjectMapper()

    @AfterEach
    fun tearDown() {
        StreamTeardown.closeAll(workers)
    }

    @Test
    fun `only a matching sensor record forwards the rule's action`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "late_record")
        publishRules(topics, ruleKey to rule("late_record", "value.time > 120.0", topics.outputTopic1, scope = ruleKey.scope))
        val streams = start(topics, stateDir)

        publishSensorRecord(topics, time = 90.0)
        publishSensorRecord(topics, time = 130.0)

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    @Test
    fun `each rule's action goes to its own topic`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        publishRules(
            topics,
            RuleKey(ScopePrefix.GLOBAL.prefix, "late") to rule("late", "value.time > 120.0", topics.outputTopic1, actionName = "foo", scope = ScopePrefix.GLOBAL.prefix),
            RuleKey(ScopePrefix.GLOBAL.prefix, "early") to rule("early", "value.time < 150.0", topics.outputTopic2, actionName = "bar", scope = ScopePrefix.GLOBAL.prefix),
        )
        val streams = start(topics, stateDir)

        listOf(130.0, 90.0, 170.0, 170.0).forEach { publishSensorRecord(topics, time = it) }

        val foo = Action(topic = topics.outputTopic1, name = "foo")
        assertEquals(listOf(foo, foo, foo), readActions(topics.outputTopic1, 3, streams))
        val bar = Action(topic = topics.outputTopic2, name = "bar")
        assertEquals(listOf(bar, bar), readActions(topics.outputTopic2, 2, streams))
    }

    @Test
    fun `an expression can compare a float field of the record value`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "high_heart_rate")
        publishRules(topics, ruleKey to rule("high_heart_rate", "value.heartRate > 120.0", topics.outputTopic1, scope = ruleKey.scope))
        val streams = start(topics, stateDir)

        publishSensorRecord(topics, heartRate = 60.0f)
        publishSensorRecord(topics, heartRate = 130.0f)

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    @Test
    fun `a project-scoped rule fires only for records of that project`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.PROJECT.prefix + "project-1", name = "always")
        publishRules(topics, ruleKey to rule("always", "true", topics.outputTopic1, scope = ruleKey.scope))
        val streams = start(topics, stateDir)

        publishSensorRecord(topics, projectId = "project-2")
        publishSensorRecord(topics, projectId = "project-1")

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    @Test
    fun `a user-scoped rule fires only for records of that user`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.USER.prefix + "user-1", name = "always")
        publishRules(topics, ruleKey to rule("always", "true", topics.outputTopic1, scope = ruleKey.scope))
        val streams = start(topics, stateDir)

        publishSensorRecord(topics, userId = "user-2")
        publishSensorRecord(topics, userId = "user-1")

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    @Test
    fun `a user-scoped rule overrides a global rule with the same name`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        publishRules(
            topics,
            RuleKey(ScopePrefix.GLOBAL.prefix, "always") to rule("always", "true", topics.outputTopic1, actionName = "global", scope = ScopePrefix.GLOBAL.prefix),
            RuleKey(ScopePrefix.USER.prefix + "user-1", "always") to rule("always", "true", topics.outputTopic1, actionName = "user", scope = ScopePrefix.USER.prefix + "user-1"),
        )
        val streams = start(topics, stateDir)

        publishSensorRecord(topics, userId = "user-1")
        publishSensorRecord(topics, userId = "user-2")

        assertEquals(
            listOf(Action(topic = topics.outputTopic1, name = "user"), Action(topic = topics.outputTopic1, name = "global")),
            readActions(topics.outputTopic1, 2, streams),
        )
    }

    @Test
    fun `a rule published while the stream runs takes effect`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val streams = start(topics, stateDir)

        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "always")
        publishRules(topics, ruleKey to rule("always", "true", topics.outputTopic1, scope = ruleKey.scope))
        awaitRule(streams, topics, ruleKey, present = true)
        publishSensorRecord(topics)

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    @Test
    fun `a deleted rule stops firing`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "always")
        publishRules(topics, ruleKey to rule("always", "true", topics.outputTopic1, scope = ruleKey.scope))
        val streams = start(topics, stateDir)

        publishSensorRecord(topics)
        OutputTopicConsumer.waitForRecordValues(KafkaBroker.bootstrapServers, topics.outputTopic1, minCount = 1, streams = streams) { it }

        RuleEngineConfigFixtures.publishTombstone(KafkaBroker.bootstrapServers, topics.globalStoreTopic, ruleKey)
        awaitRule(streams, topics, ruleKey, present = false)
        publishSensorRecord(topics)

        assertEquals(listOf(Action(topic = topics.outputTopic1, name = "notify")), readActions(topics.outputTopic1, 1, streams))
    }

    private fun start(topics: Topics, stateDir: Path): KafkaStreams {
        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.all())
        val streams = RuleEngineWorkers.start(topics, stateDir, workers, schemaRegistryUrl(topics))
        StreamReadiness.awaitRunning(listOf(streams))
        return streams
    }

    private fun publishRules(topics: Topics, vararg rules: Pair<RuleKey, Rule>) {
        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.all())
        rules.forEachIndexed { index, (key, rule) ->
            RuleEngineConfigFixtures.publishRule(KafkaBroker.bootstrapServers, topics.globalStoreTopic, index + 1, key, rule)
        }
    }

    private fun awaitRule(streams: KafkaStreams, topics: Topics, ruleKey: RuleKey, present: Boolean) {
        val group = StateStoreQueries.waitUntil<RuleGroup>(streams, topics.globalStoreName, ruleKey.scope) {
            (it?.rules?.containsKey(ruleKey.name) == true) == present
        }
        assertEquals(present, group?.rules?.containsKey(ruleKey.name) == true, "Rule $ruleKey present in store")
    }

    /**
     * Waits for [expectedCount] actions on [topic], then reads the topic again for [QUIET_PERIOD] to also catch
     * unexpected actions. Input records are processed in order, so an action from an earlier, non-matching record
     * is already on the topic when the expected ones arrive.
     */
    private fun readActions(topic: String, expectedCount: Int, streams: KafkaStreams): List<Action> {
        OutputTopicConsumer.waitForRecordValues(KafkaBroker.bootstrapServers, topic, expectedCount, streams = streams) { it }
        return OutputTopicConsumer.waitForRecordValues(
            bootstrapServers = KafkaBroker.bootstrapServers,
            topic = topic,
            minCount = expectedCount + 1,
            timeout = QUIET_PERIOD,
            streams = streams,
        ) { bytes -> mapper.readValue(bytes, Action::class.java) }
    }

    private fun publishSensorRecord(
        topics: Topics,
        projectId: String = "project-1",
        userId: String = "user-1",
        time: Double = 100.0,
        heartRate: Float = 60.0f,
    ) {
        val serializerConfig = mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl(topics))
        val keySerializer = KafkaAvroSerializer().apply { configure(serializerConfig, true) }
        val valueSerializer = KafkaAvroSerializer().apply { configure(serializerConfig, false) }

        val key = ObservationKey(projectId, userId, "source-1")
        val value = Pebble2HeartRate(time, time, heartRate)

        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBroker.bootstrapServers)
        }
        KafkaProducer(props, keySerializer, valueSerializer).use { producer ->
            producer.send(ProducerRecord(topics.inputTopic, key, value)).get()
        }
    }

    private fun schemaRegistryUrl(topics: Topics) = "mock://${topics.globalStoreName}"

    private companion object {
        /** How long a test keeps listening for unexpected actions after the expected ones arrived. */
        val QUIET_PERIOD: Duration = Duration.ofSeconds(1)
    }
}
