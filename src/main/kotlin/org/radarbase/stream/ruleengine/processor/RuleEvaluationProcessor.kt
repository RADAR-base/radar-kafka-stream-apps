package org.radarbase.stream.ruleengine.processor

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.ContextualProcessor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.evaluator.ConditionFactory
import org.slf4j.LoggerFactory

// TODO: deploy on cluster
class RuleEvaluationProcessor(
    val globalStoreName: String,
) : ContextualProcessor<GenericRecord, GenericRecord, GenericRecord, Action>() {
    companion object {
        private val logger = LoggerFactory.getLogger(RuleEvaluationProcessor::class.java)
        const val TARGET_TOPIC_HEADER = "rule-engine-target-topic"
    }

    private lateinit var groupedRulesStore: ReadOnlyKeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<GenericRecord, Action>) {
        super.init(context)
        groupedRulesStore = context().getStateStore(globalStoreName)
    }

    override fun process(record: Record<GenericRecord, GenericRecord>) {
        val recordKeyAvro = record.key()
        val recordValueAvro = record.value()

        if (recordKeyAvro == null || recordValueAvro == null) return

        val topic = context().recordMetadata().get().topic()

        val recordKey = recordKeyAvro.toCelCompatible()
        val recordValue = recordValueAvro.toCelCompatible()

        logger.debug("Processing record from topic {} with key {} and value {}", topic, recordKey, recordValue)

        val ruleGroup: RuleGroup = groupedRulesStore.get(ScopePrefix.GLOBAL.scope) ?: return
        // TODO: do something with Scope

        ruleGroup.rules.forEach { (_, rule) ->
            val result = evaluateRule(rule, recordKey, recordValue)
            if (result) {
                rule.actions
                    .forEach { action ->
                        val headers = RecordHeaders().add(TARGET_TOPIC_HEADER, rule.topic.toByteArray(Charsets.UTF_8))
                        context().forward(Record(null, action, record.timestamp(), headers))
                    }
            }
        }
    }

    private fun evaluateRule(
        rule: Rule,
        recordKey: Map<String, Any?>,
        recordValue: Map<String, Any?>,
    ): Boolean {
        val applicableConditions = rule.conditions
        try {
            return applicableConditions.any { condition ->
                val evaluator = ConditionFactory.getConditionEvaluator(condition)
                evaluator.isTrueFor(recordKey, recordValue, condition.expression)
            }
        } catch (e: Exception) {
            logger.warn("Failed to evaluate rule {}: {}", rule.name, e.message)
            return false
        }
    }

    fun GenericRecord.toCelCompatible(): Map<String, Any?> =
        this.schema.fields.associate { it.name() to this.get(it.pos())?.toCelCompatible() }

    fun Any.toCelCompatible(): Any = when (this) {
        is CharSequence -> this.toString() // Converts Avro Utf8 to java.lang.String
        is List<*> -> this.map { it?.toCelCompatible() }
        is Map<*, *> -> this.entries.associate { it.key.toString() to it.value?.toCelCompatible() }
        else -> this
    }
}
