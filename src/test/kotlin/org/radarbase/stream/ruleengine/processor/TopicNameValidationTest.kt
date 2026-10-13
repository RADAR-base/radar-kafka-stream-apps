package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleKey
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TopicNameValidationTest {

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
    fun validName() {
        processor.process(Record(ruleKey(), rule("alerts"), 0L))
        assertNotNull(store.get("global"))
    }

    @Test
    fun validNameWithSpecialCharacters() {
        processor.process(Record(ruleKey(), rule("intervention_v1.alerts-2"), 0L))
        assertNotNull(store.get("global"))
    }

    @Test
    fun validNameWithMaxLength() {
        processor.process(Record(ruleKey(), rule("x".repeat(249)), 0L))
        assertNotNull(store.get("global"))
    }

    @Test
    fun invalidNameEmpty() {
        processor.process(Record(ruleKey(), rule(""), 0L))
        assertNull(store.get("global"))
    }

    @Test
    fun invalidNameSlash() {
        processor.process(Record(ruleKey(), rule("/"), 0L))
        assertNull(store.get("global"))
    }

    @Test
    fun invalidNameSpace() {
        processor.process(Record(ruleKey(), rule("a b"), 0L))
        assertNull(store.get("global"))
    }

    @Test
    fun invalidNameTooLong() {
        processor.process(Record(ruleKey(), rule("x".repeat(250)), 0L))
        assertNull(store.get("global"))
    }

    private fun ruleKey() = RuleKey(scope = "global", name = "rule-1")

    private fun rule(
        topic: String,
    ) = Rule(
        name = "rule-1",
        condition =
            Condition(
                type = ExpressionType.CEL,
                expression = "true",
                name = "condition",
            ),
        action = Action(
            name = "notify",
            topic = topic
        ),
    )
}
