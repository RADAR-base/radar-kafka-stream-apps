package org.radarbase.stream.ruleengine.processor

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.ContextualProcessor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Rule
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

    private lateinit var store: RuleStore

    override fun init(context: ProcessorContext<GenericRecord, Action>) {
        super.init(context)
        store = RuleStore(context().getStateStore(globalStoreName))
    }

    override fun process(record: Record<GenericRecord, GenericRecord>) {
        val recordKeyAvro = record.key()
        val recordValueAvro = record.value()

        if (recordKeyAvro == null || recordValueAvro == null) return

        val topic = context().recordMetadata().get().topic()

        val recordKey = recordKeyAvro.toCelCompatible()
        val recordValue = recordValueAvro.toCelCompatible()

        logger.debug("Processing record from topic {} with key {} and value {}", topic, recordKey, recordValue)

        val userId = recordKeyAvro["userId"]?.toString() ?: return logger.debug("record does not have userId")
        val projectId = recordKeyAvro["projectId"]?.toString() ?: return logger.debug("record does not have projectId")

        store.getRules(userId, projectId).forEach { rule ->
            val result = evaluateRule(rule, recordKey, recordValue)
            if (result) {
                val headers = RecordHeaders().add(TARGET_TOPIC_HEADER, rule.action.topic.toByteArray(Charsets.UTF_8))
                context().forward(Record(null, rule.action, record.timestamp(), headers))
            }
        }
    }

    private fun evaluateRule(
        rule: Rule,
        recordKey: Map<String, Any?>,
        recordValue: Map<String, Any?>,
    ): Boolean {
        try {
            val evaluator = ConditionFactory.getConditionEvaluator(rule.condition)
            return evaluator.isTrueFor(recordKey, recordValue, rule.condition.expression)
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
        is Float -> this.toDouble()
        is Int, is Short -> (this as Number).toLong()
        else -> this
    }
}
