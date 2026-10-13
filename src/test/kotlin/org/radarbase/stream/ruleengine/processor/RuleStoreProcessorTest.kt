package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.ScopePrefix
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class RuleStoreProcessorTest {

    private lateinit var store: FakeRuleGroupStore
    private lateinit var processor: RuleStoreProcessor

    @BeforeEach
    fun setUp() {
        store = FakeRuleGroupStore()

        @Suppress("UNCHECKED_CAST")
        val context = mock(ProcessorContext::class.java) as ProcessorContext<Void, Void>
        `when`(context.getStateStore<FakeRuleGroupStore>(any())).thenReturn(store)

        processor = RuleStoreProcessor("store-name")
        processor.init(context)
    }

    @Test
    fun `a rule is stored under the scope and name of its key`() {
        listOf(GLOBAL, PROJECT_1, USER_1).forEach { scope ->
            val config = rule(name = "rule1")
            processor.process(Record(RuleKey(scope, "rule1"), config, 0L))

            assertEquals(mapOf("rule1" to config), store.get(scope)?.rules)
        }
    }

    @Test
    fun `the key name is used as the store key`() {
        val config = rule(name = "name_in_value")
        processor.process(Record(RuleKey(GLOBAL, "name_in_key"), config, 0L))

        assertEquals(setOf("name_in_key"), store.get(GLOBAL)?.rules?.keys)
    }

    @Test
    fun `multiple rules aggregate into the same scope`() {
        processor.process(Record(RuleKey(PROJECT_1, "rule_a"), rule(name = "rule_a"), 0L))
        processor.process(Record(RuleKey(PROJECT_1, "rule_b"), rule(name = "rule_b"), 0L))

        assertEquals(setOf("rule_a", "rule_b"), store.get(PROJECT_1)?.rules?.keys)
    }

    @Test
    fun `a rule with the same key replaces the previous version`() {
        processor.process(Record(RuleKey(GLOBAL, "rule1"), rule(name = "rule1", topic = "old_topic"), 0L))
        processor.process(Record(RuleKey(GLOBAL, "rule1"), rule(name = "rule1", topic = "new_topic"), 0L))

        assertEquals("new_topic", store.get(GLOBAL)?.rules?.get("rule1")?.action?.topic)
    }

    @Test
    fun `rules with the same name in different scopes are stored independently`() {
        val projectRule = rule(name = "rule1", topic = "project_topic")
        val userRule = rule(name = "rule1", topic = "user_topic")
        processor.process(Record(RuleKey(PROJECT_1, "rule1"), projectRule, 0L))
        processor.process(Record(RuleKey(USER_1, "rule1"), userRule, 0L))

        assertEquals(projectRule, store.get(PROJECT_1)?.rules?.get("rule1"))
        assertEquals(userRule, store.get(USER_1)?.rules?.get("rule1"))
    }

    @Test
    fun `a tombstone removes the rule only from the scope of its key`() {
        processor.process(Record(RuleKey(PROJECT_1, "rule1"), rule(name = "rule1"), 0L))
        processor.process(Record(RuleKey(PROJECT_1, "rule2"), rule(name = "rule2"), 0L))
        processor.process(Record(RuleKey(USER_1, "rule1"), rule(name = "rule1"), 0L))

        processor.process(Record(RuleKey(PROJECT_1, "rule1"), null, 0L))

        assertEquals(setOf("rule2"), store.get(PROJECT_1)?.rules?.keys)
        assertEquals(setOf("rule1"), store.get(USER_1)?.rules?.keys)
    }

    @Test
    fun `a tombstone for the last rule of a scope leaves no rules in that scope`() {
        processor.process(Record(RuleKey(PROJECT_1, "rule1"), rule(name = "rule1"), 0L))

        processor.process(Record(RuleKey(PROJECT_1, "rule1"), null, 0L))

        assertEquals(emptyMap(), store.get(PROJECT_1)?.rules.orEmpty())
    }


    @Test
    fun `an update with an invalid topic removes the previously stored rule`() {
        processor.process(Record(RuleKey(GLOBAL, "rule1"), rule(name = "rule1", topic = "valid_topic"), 0L))

        processor.process(Record(RuleKey(GLOBAL, "rule1"), rule(name = "rule1", topic = "invalid topic"), 0L))

        assertFalse(store.get(GLOBAL)?.rules.orEmpty().containsKey("rule1"))
    }

    @Test
    fun `a record with a null key is ignored`() {
        processor.process(Record(null, rule(name = "rule1"), 0L))

        assertEquals(0L, store.approximateNumEntries())
    }

    private fun rule(
        name: String,
        topic: String = "output_topic",
    ) = Rule(
        name = name,
        condition = Condition(
            type = ExpressionType.CEL,
            expression = "true",
            name = "${name}_condition",
        ),
        action = Action(
            topic = topic,
            name = "notify",
        ),
    )

    companion object {
        private val GLOBAL = ScopePrefix.GLOBAL.prefix
        private val PROJECT_1 = ScopePrefix.PROJECT.prefix + "project-1"
        private val USER_1 = ScopePrefix.USER.prefix + "user-1"
    }
}
