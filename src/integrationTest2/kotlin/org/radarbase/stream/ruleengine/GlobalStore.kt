package org.radarbase.stream.ruleengine

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.radarbase.config.SingleStreamConfig
import org.radarbase.stream.StreamMaster
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.harness.FixtureRadarConfigHandler
import org.radarbase.stream.ruleengine.harness.KafkaBroker
import org.radarbase.stream.ruleengine.harness.KafkaBrokerExtension
import org.radarbase.stream.ruleengine.harness.KafkaTopics
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.StateStoreQueries
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import org.radarbase.stream.ruleengine.harness.StreamTeardown
import org.radarbase.stream.ruleengine.harness.Topics
import org.radarbase.stream.ruleengine.processor.ScopePrefix
import java.nio.file.Path
import java.util.stream.Stream

@ExtendWith(KafkaBrokerExtension::class)
class GlobalStore {

    private val workers = mutableListOf<RuleEngineStream>()

    @AfterEach
    fun tearDown() {
        StreamTeardown.closeAll(workers)
    }

    @Test
    fun `global store is populated from a global-scope rule`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = "config", name = "high_heart_rate")
        val rule = rule(name = "high_heart_rate", topic = topics.outputTopic1, expression = "value.heartRate > 120")

        publish(topics, id = 1, ruleKey = ruleKey, rule = rule)

        val streams = startWorker(topics, stateDir)
        StreamReadiness.awaitRunning(listOf(streams))

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ScopePrefix.GLOBAL.scope)

        assertNotNull(ruleGroup, "Expected the global-scope rule to land in the store under the '${ScopePrefix.GLOBAL.scope}' key")
        assertEquals(setOf(ruleKey.toStoreKey()), ruleGroup!!.rules.keys)
    }

    @Test
    fun `global store restore survives malformed legacy JSON and keeps the later well-formed record`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = "project.test-questionnaire", name = "rule4")
        val rule = rule(name = "rule4", topic = topics.outputTopic1, expression = "value.heartRate > 120")

        val malformedJson = """
            {"name":"rule4","topic":"${topics.outputTopic1}","conditions":[{"type":"cel","expression":"value.heartRate > 120"       "projects":["STAGING_PROJECT"],"subjects":null,"name":"rule4_condition"}],"actions":[{"name":"notify"}]}
        """.trimIndent()

        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.all())
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey),
            RuleEngineConfigFixtures.valueRaw(id = 1, key = ruleKey, rawJson = malformedJson),
        )
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey),
            RuleEngineConfigFixtures.value(id = 2, key = ruleKey, rule = rule),
        )

        val streams = startWorker(topics, stateDir)

        StreamReadiness.awaitRunning(listOf(streams))

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ScopePrefix.GLOBAL.scope)

        assertNotNull(ruleGroup, "Expected the later well-formed record to land in the store under the '${ScopePrefix.GLOBAL.scope}' key")
        assertEquals(rule, ruleGroup!!.rules[ruleKey.toStoreKey()])
    }

    private fun publish(topics: Topics, id: Int, ruleKey: RuleKey, rule: Rule) {
        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.all())
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey),
            RuleEngineConfigFixtures.value(id = id, key = ruleKey, rule = rule),
        )
    }

    private fun rule(name: String, topic: String, expression: String) = Rule(
        name = name,
        topic = topic,
        conditions = listOf(
            Condition(type = ExpressionType.CEL, expression = expression, name = "${name}_condition"),
        ),
        actions = listOf(Action(name = "notify")),
    )

    private fun startWorker(topics: Topics, stateDir: Path): KafkaStreams {
        val singleStreamConfig = SingleStreamConfig().apply {
            streamClass = RuleEngineStream::class.java
            properties = mapOf(
                "input_topic" to topics.inputTopic,
                "output_topic" to topics.outputTopic1,
                "global_store_name" to topics.globalStoreName,
                "global_store_topic" to topics.globalStoreTopic,
                StreamsConfig.STATE_DIR_CONFIG to stateDir.toString(),
                StreamsConfig.APPLICATION_ID_CONFIG to "rule-engine-it-${topics.globalStoreName}",
            )
        }

        val configHandler = FixtureRadarConfigHandler(KafkaBroker.bootstrapServers)
        val master = StreamMaster(configHandler, Stream.empty())
        val worker = RuleEngineStream()
        worker.configure(master, configHandler, singleStreamConfig)
        workers += worker
        worker.start()

        return requireNotNull(worker.kafkaStreamsInstances) { "RuleEngineStream did not build any KafkaStreams instances" }.first()
    }
}
