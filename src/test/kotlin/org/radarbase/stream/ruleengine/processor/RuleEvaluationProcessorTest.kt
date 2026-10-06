package org.radarbase.stream.ruleengine.processor

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
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
import java.util.Optional
import kotlin.test.assertEquals

class RuleEvaluationProcessorTest {

    private lateinit var store: FakeRuleGroupStore
    private lateinit var context: ProcessorContext<GenericRecord, Action>
    private lateinit var processor: RuleEvaluationProcessor

    @BeforeEach
    fun setUp() {
        store = FakeRuleGroupStore()

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
    fun `a global-scoped rule matches regardless of key`() {
        seed(ScopePrefix.GLOBAL.scope, "rule1", rule("rule1", listOf(condition("value.heartRate > 120.0"))))

        processor.process(Record(observationKey(), sensorValue(130.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a rule present in multiple scope buckets fires only once`() {
        val config = rule(
            "rule1",
            listOf(
                condition("true", name = "global_cond"),
                condition("true", name = "project_cond", projects = listOf("project-1")),
            ),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)
        seed("project.project-1", "rule1", config)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `one output record is forwarded per matched action`() {
        val config = rule(
            "rule1",
            listOf(condition("true")),
            actions = listOf(Action(name = "action-a"), Action(name = "action-b")),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        val actionNames = forwardedActions().map { it.value().name }
        assertEquals(listOf("action-a", "action-b"), actionNames)
    }

    @Test
    fun `a malformed expression in one rule does not block other rules from firing`() {
        val badConfig = rule(
            "bad_rule",
            listOf(condition("value.. invalid((", name = "bad_cond")),
            actions = listOf(Action(name = "bad_action")),
        )
        val goodConfig = rule(
            "good_rule",
            listOf(condition("true", name = "good_cond")),
            actions = listOf(Action(name = "good_action")),
        )
        seed(ScopePrefix.GLOBAL.scope, "bad_rule", badConfig)
        seed(ScopePrefix.GLOBAL.scope, "good_rule", goodConfig)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        val actionNames = forwardedActions().map { it.value().name }
        assertEquals(listOf("good_action"), actionNames)
    }

    @Test
    fun `forward topic in header`() {
        val rule = rule(
            "rule",
            listOf(condition("true", name = "condition")),
            actions = listOf(Action(name = "action")),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule", rule)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(listOf("output_topic"), forwardedActions().map { it.targetTopic() })
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
        conditions: List<Condition>,
        actions: List<Action> = listOf(Action(name = "notify")),
    ) = Rule(name = name, topic = "output_topic", conditions = conditions, actions = actions)

    private fun condition(
        expression: String,
        name: String = "cond",
        projects: List<String>? = null,
        subjects: List<String>? = null,
    ) = Condition(type = ExpressionType.CEL, expression = expression, name = name, projects = projects, subjects = subjects)

    private fun observationKey(projectId: String? = null, userId: String? = null, sourceId: String? = null): GenericRecord =
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
