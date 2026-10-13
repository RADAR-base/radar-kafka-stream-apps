package org.radarbase.stream.ruleengine.processor

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.processor.api.RecordMetadata
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.any
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.ScopePrefix
import java.util.Optional
import kotlin.test.assertEquals

class RuleEvaluationProcessorTest {

    private lateinit var store: FakeRuleGroupStore
    private lateinit var context: ProcessorContext<GenericRecord, Action>
    private lateinit var processor: RuleEvaluationProcessor

    @BeforeEach
    fun setUp() {
        store = FakeRuleGroupStore()
        // Every scope the default record key resolves to has a group, so that tests do not depend on
        // how missing groups are handled. That case has its own test.
        listOf(GLOBAL, PROJECT_1, USER_1).forEach { store.putDirect(it, RuleGroup()) }

        @Suppress("UNCHECKED_CAST")
        val mockContext = mock(ProcessorContext::class.java) as ProcessorContext<GenericRecord, Action>
        `when`(mockContext.getStateStore<FakeRuleGroupStore>(any())).thenReturn(store)
        `when`(mockContext.recordMetadata()).thenReturn(
            Optional.of(
                object : RecordMetadata {
                    override fun topic(): String = "input_topic"
                    override fun partition(): Int = 0
                    override fun offset(): Long = 0
                },
            ),
        )
        context = mockContext

        processor = RuleEvaluationProcessor("store-name")
        processor.init(context)
    }

    @Test
    fun `a matching rule forwards its action`() {
        val action = Action(topic = "output_topic", name = "notify", properties = mapOf("level" to "high"))
        seed(GLOBAL, "rule1", rule("rule1", condition("value.heartRate > 120.0"), action))

        processor.process(Record(observationKey(), sensorValue(130.0), 0L))

        assertEquals(listOf(action), forwardedActions().map { it.value() })
    }

    @Test
    fun `a non-matching rule forwards nothing`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("value.heartRate > 120.0")))

        processor.process(Record(observationKey(), sensorValue(100.0), 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `the expression can read fields of the record key`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("key.userId == 'user-1' && key.projectId == 'project-1'")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `the forwarded record carries the action topic in a header and keeps the input timestamp`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true"), Action(topic = "alerts", name = "notify")))

        processor.process(Record(observationKey(), sensorValue(0.0), 1234L))

        val forwarded = forwardedActions().single()
        assertEquals("alerts", forwarded.targetTopic())
        assertEquals(1234L, forwarded.timestamp())
    }

    @Test
    fun `every matching rule forwards its own action`() {
        seed(GLOBAL, "rule_a", rule("rule_a", condition("true"), Action(topic = "topic_a", name = "action_a")))
        seed(GLOBAL, "rule_b", rule("rule_b", condition("true"), Action(topic = "topic_b", name = "action_b")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(listOf("topic_a", "topic_b"), forwardedActions().map { it.targetTopic() })
    }

    @Test
    fun `a malformed expression in one rule does not block other rules from firing`() {
        seed(GLOBAL, "bad_rule", rule("bad_rule", condition("value.. invalid(("), Action(topic = "out", name = "bad_action")))
        seed(GLOBAL, "good_rule", rule("good_rule", condition("true"), Action(topic = "out", name = "good_action")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(listOf("good_action"), forwardedActions().map { it.value().name })
    }

    @Test
    fun `an expression that does not evaluate to a boolean does not fire`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("value.heartRate")))

        processor.process(Record(observationKey(), sensorValue(130.0), 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `a project-scoped rule fires only for records of that project`() {
        store.putDirect(ScopePrefix.PROJECT.prefix + "project-2", RuleGroup())
        seed(PROJECT_1, "rule1", rule("rule1", condition("true")))

        processor.process(Record(observationKey(projectId = "project-2"), sensorValue(0.0), 0L))
        assertEquals(0, forwardedActions().size)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))
        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a user-scoped rule fires only for records of that user`() {
        store.putDirect(ScopePrefix.USER.prefix + "user-2", RuleGroup())
        seed(USER_1, "rule1", rule("rule1", condition("true")))

        processor.process(Record(observationKey(userId = "user-2"), sensorValue(0.0), 0L))
        assertEquals(0, forwardedActions().size)

        processor.process(Record(observationKey(userId = "user-1"), sensorValue(0.0), 0L))
        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a project-scoped rule overrides a global rule with the same name`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "global_action")))
        seed(PROJECT_1, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "project_action")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(listOf("project_action"), forwardedActions().map { it.value().name })
    }

    @Test
    fun `a user-scoped rule overrides project and global rules with the same name`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "global_action")))
        seed(PROJECT_1, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "project_action")))
        seed(USER_1, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "user_action")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(listOf("user_action"), forwardedActions().map { it.value().name })
    }

    @Test
    fun `an overriding rule that does not match suppresses the overridden rule`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true"), Action(topic = "out", name = "global_action")))
        seed(USER_1, "rule1", rule("rule1", condition("false"), Action(topic = "out", name = "user_action")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `rules with different names in different scopes all fire`() {
        seed(GLOBAL, "global_rule", rule("global_rule", condition("true"), Action(topic = "out", name = "global_action")))
        seed(PROJECT_1, "project_rule", rule("project_rule", condition("true"), Action(topic = "out", name = "project_action")))
        seed(USER_1, "user_rule", rule("user_rule", condition("true"), Action(topic = "out", name = "user_action")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(
            setOf("global_action", "project_action", "user_action"),
            forwardedActions().map { it.value().name }.toSet(),
        )
    }

    @Test
    fun `a record without userId or projectId is skipped`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true")))

        processor.process(Record(observationKey(userId = null), sensorValue(0.0), 0L))
        processor.process(Record(observationKey(projectId = null), sensorValue(0.0), 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `a record with a null key or value is skipped`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true")))

        processor.process(Record(null, sensorValue(0.0), 0L))
        processor.process(Record(observationKey(), null, 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `rules are evaluated when some scopes have no rule group`() {
        store.delete(PROJECT_1)
        store.delete(USER_1)
        seed(GLOBAL, "rule1", rule("rule1", condition("true")))

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `key fields decoded as Avro Utf8 are recognised`() {
        seed(GLOBAL, "rule1", rule("rule1", condition("true")))
        val key = GenericData.Record(KEY_SCHEMA).apply {
            put("projectId", Utf8("project-1"))
            put("userId", Utf8("user-1"))
            put("sourceId", Utf8("source-1"))
        }

        processor.process(Record(key, sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    private fun Record<*, *>.targetTopic(): String? =
        headers().lastHeader(RuleEvaluationProcessor.TARGET_TOPIC_HEADER)?.value()?.toString(Charsets.UTF_8)

    private fun seed(scopeKey: String, storeKey: String, config: Rule) {
        val rules = store.get(scopeKey)?.rules.orEmpty() + (storeKey to config)
        store.putDirect(scopeKey, RuleGroup(rules))
    }

    @Suppress("UNCHECKED_CAST")
    private fun forwardedActions(): List<Record<GenericRecord, Action>> {
        val captor = ArgumentCaptor.forClass(Record::class.java) as ArgumentCaptor<Record<GenericRecord, Action>>
        verify(context, atLeast(0)).forward(captor.capture())
        return captor.allValues
    }

    private fun rule(
        name: String,
        condition: Condition,
        action: Action = Action(topic = "output_topic", name = "notify"),
    ) = Rule(name = name, condition = condition, action = action)

    private fun condition(expression: String) =
        Condition(type = ExpressionType.CEL, expression = expression, name = "trigger")

    private fun observationKey(
        projectId: String? = "project-1",
        userId: String? = "user-1",
        sourceId: String? = "source-1",
    ): GenericRecord =
        GenericData.Record(KEY_SCHEMA).apply {
            put("projectId", projectId)
            put("userId", userId)
            put("sourceId", sourceId)
        }

    private fun sensorValue(heartRate: Double): GenericRecord =
        GenericData.Record(VALUE_SCHEMA).apply {
            put("heartRate", heartRate)
        }

    companion object {
        private val GLOBAL = ScopePrefix.GLOBAL.prefix
        private val PROJECT_1 = ScopePrefix.PROJECT.prefix + "project-1"
        private val USER_1 = ScopePrefix.USER.prefix + "user-1"

        private val KEY_SCHEMA: Schema = Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "ObservationKey",
              "namespace": "org.radarbase.stream.ruleengine.test",
              "fields": [
                {"name": "projectId", "type": ["null", "string"], "default": null},
                {"name": "userId", "type": ["null", "string"], "default": null},
                {"name": "sourceId", "type": ["null", "string"], "default": null}
              ]
            }
            """.trimIndent(),
        )

        private val VALUE_SCHEMA: Schema = Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "TestSensorValue",
              "namespace": "org.radarbase.stream.ruleengine.test",
              "fields": [
                {"name": "heartRate", "type": "double"}
              ]
            }
            """.trimIndent(),
        )
    }
}
