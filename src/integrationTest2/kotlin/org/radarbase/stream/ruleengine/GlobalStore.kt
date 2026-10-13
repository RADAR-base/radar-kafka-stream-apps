package org.radarbase.stream.ruleengine

import org.apache.kafka.streams.KafkaStreams
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.ScopePrefix
import org.radarbase.stream.ruleengine.harness.KafkaBroker
import org.radarbase.stream.ruleengine.harness.KafkaBrokerExtension
import org.radarbase.stream.ruleengine.harness.KafkaTopics
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.RuleEngineWorkers
import org.radarbase.stream.ruleengine.harness.RuleFixtures
import org.radarbase.stream.ruleengine.harness.StateStoreQueries
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import org.radarbase.stream.ruleengine.harness.StreamTeardown
import org.radarbase.stream.ruleengine.harness.Topics
import java.nio.file.Path

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
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "high_heart_rate")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = rule("high_heart_rate", ruleKey.scope, topics))
        val streams = start(topics, stateDir)

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ruleKey.scope)

        assertNotNull(ruleGroup, "Expected the global-scope rule to land in the store under the '${ruleKey.scope}' key")
        assertEquals(setOf(ruleKey.name), ruleGroup!!.rules.keys)
    }

    @Test
    fun `rules are stored under the project or user scope of their key`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val projectKey = RuleKey(scope = ScopePrefix.PROJECT.prefix + "project-1", name = "project_rule")
        val userKey = RuleKey(scope = ScopePrefix.USER.prefix + "user-1", name = "user_rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = projectKey, rule = rule("project_rule", projectKey.scope, topics))
        publish(topics, id = 2, ruleKey = userKey, rule = rule("user_rule", userKey.scope, topics))
        val streams = start(topics, stateDir)

        assertEquals(setOf(projectKey.name), awaitGroup(streams, topics, projectKey.scope).rules.keys)
        assertEquals(setOf(userKey.name), awaitGroup(streams, topics, userKey.scope).rules.keys)
    }

    @Test
    fun `rules in the same scope aggregate into one group`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val keyA = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule_a")
        val keyB = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule_b")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = keyA, rule = rule("rule_a", keyA.scope, topics))
        publish(topics, id = 2, ruleKey = keyB, rule = rule("rule_b", keyB.scope, topics))
        val streams = start(topics, stateDir)

        val group = StateStoreQueries.waitUntil<RuleGroup>(streams, topics.globalStoreName, ScopePrefix.GLOBAL.prefix) {
            it?.rules?.size == 2
        }
        assertEquals(setOf("rule_a", "rule_b"), group?.rules?.keys)
    }

    @Test
    fun `a later record with the same key replaces the rule`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = RuleFixtures.rule("rule", "true", topics.outputTopic1, scope = ruleKey.scope))
        publish(topics, id = 1, ruleKey = ruleKey, rule = RuleFixtures.rule("rule", "true", topics.outputTopic2, scope = ruleKey.scope))
        publishSentinel(topics)
        val streams = start(topics, stateDir)

        val group = awaitSentinel(streams, topics)
        assertEquals(topics.outputTopic2, group.rules[ruleKey.name]?.action?.topic)
    }

    @Test
    fun `a tombstone removes the rule from its scope`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = rule("rule", ruleKey.scope, topics))
        RuleEngineConfigFixtures.publishTombstone(KafkaBroker.bootstrapServers, topics.globalStoreTopic, ruleKey)
        publishSentinel(topics)
        val streams = start(topics, stateDir)

        assertEquals(setOf(SENTINEL.name), awaitSentinel(streams, topics).rules.keys)
    }

    @Test
    fun `a row without a value removes the rule from its scope`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = rule("rule", ruleKey.scope, topics))
        publishRaw(topics, id = 1, ruleKey = ruleKey, rawJson = null)
        publishSentinel(topics)
        val streams = start(topics, stateDir)

        assertEquals(setOf(SENTINEL.name), awaitSentinel(streams, topics).rules.keys)
    }

    @Test
    fun `a row without a scope removes the rule from the scope of its key`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = rule("rule", ruleKey.scope, topics))
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey),
            RuleEngineConfigFixtures.value(id = 1, key = ruleKey, rule = rule("rule", ruleKey.scope, topics), scope = null),
        )
        publishSentinel(topics)
        val streams = start(topics, stateDir)

        assertEquals(setOf(SENTINEL.name), awaitSentinel(streams, topics).rules.keys)
    }

    @Test
    fun `a rule with an invalid output topic name is not stored`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "rule")

        createTopics(topics)
        publish(topics, id = 1, ruleKey = ruleKey, rule = RuleFixtures.rule("rule", "true", "invalid topic", scope = ruleKey.scope))
        publishSentinel(topics)
        val streams = start(topics, stateDir)

        assertEquals(setOf(SENTINEL.name), awaitSentinel(streams, topics).rules.keys)
    }

    @Test
    fun `global store restore survives malformed JSON and keeps the later well-formed record`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(scope = "project.test-questionnaire", name = "rule4")
        val rule = rule("rule4", ruleKey.scope, topics)

        val malformedJson = """
            {"name":"rule4","condition":{"type":"CEL","expression":"value.heartRate > 120"       "name":"rule4_condition"},"action":{"topic":"${topics.outputTopic1}","name":"notify"}}
        """.trimIndent()

        createTopics(topics)
        publishRaw(topics, id = 1, ruleKey = ruleKey, rawJson = malformedJson)
        publish(topics, id = 2, ruleKey = ruleKey, rule = rule)
        val streams = start(topics, stateDir)

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ruleKey.scope)

        assertNotNull(ruleGroup, "Expected the later well-formed record to land in the store under the '${ruleKey.scope}' key")
        assertEquals(rule, ruleGroup!!.rules[ruleKey.name])
    }

    private fun rule(name: String, scope: String, topics: Topics) =
        RuleFixtures.rule(name, "value.heartRate > 120", topics.outputTopic1, scope = scope)

    private fun createTopics(topics: Topics) = KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.all())

    private fun start(topics: Topics, stateDir: Path): KafkaStreams {
        val streams = RuleEngineWorkers.start(topics, stateDir, workers)
        StreamReadiness.awaitRunning(listOf(streams))
        return streams
    }

    private fun publish(topics: Topics, id: Int, ruleKey: RuleKey, rule: Rule) =
        RuleEngineConfigFixtures.publishRule(KafkaBroker.bootstrapServers, topics.globalStoreTopic, id, ruleKey, rule)

    private fun publishRaw(topics: Topics, id: Int, ruleKey: RuleKey, rawJson: String?) =
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey),
            RuleEngineConfigFixtures.valueRaw(id = id, key = ruleKey, rawJson = rawJson),
        )

    /**
     * Publishes a rule in the global scope after the records under test. The config topic has one partition, so once
     * the sentinel is in the store, every earlier record has been applied too.
     */
    private fun publishSentinel(topics: Topics) = publish(topics, id = 99, ruleKey = SENTINEL, rule = rule(SENTINEL.name, SENTINEL.scope, topics))

    private fun awaitSentinel(streams: KafkaStreams, topics: Topics): RuleGroup {
        val group = StateStoreQueries.waitUntil<RuleGroup>(streams, topics.globalStoreName, SENTINEL.scope) {
            it?.rules?.containsKey(SENTINEL.name) == true
        }
        assertNotNull(group?.rules?.get(SENTINEL.name), "Expected the sentinel rule to land in the store")
        return group!!
    }

    private fun awaitGroup(streams: KafkaStreams, topics: Topics, scope: String): RuleGroup {
        val group: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, scope)
        assertNotNull(group, "Expected a rule group under the '$scope' key")
        return group!!
    }

    private companion object {
        val SENTINEL = RuleKey(scope = ScopePrefix.GLOBAL.prefix, name = "sentinel")
    }
}
