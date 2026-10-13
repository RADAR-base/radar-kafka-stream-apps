package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.slf4j.LoggerFactory

class RuleStoreProcessor(
    val globalStoreName: String,
) : Processor<RuleKey, Rule, Void, Void> {
    private val regex = Regex("^[a-zA-Z0-9._-]*$")
    private lateinit var store: RuleStore

    override fun init(context: ProcessorContext<Void, Void>) {
        store = RuleStore(context.getStateStore(globalStoreName))
    }

    override fun process(record: Record<RuleKey, Rule>) {
        val key = record.key() ?: return
        val rule = record.value()

        if (rule == null) {
            logger.info("Received tombstone for rule key {}; removed from all scopes", key)
            store.remove(key)
            return
        }
        if (isInvalidTopicName(rule.action.topic)) {
            logger.info("Received invalid topic name {}", rule.action.topic)
            store.remove(key)
            return
        }
        store.add(key, rule)
    }

    private fun isInvalidTopicName(topicName: String): Boolean {
        if (topicName.isBlank()) {
            return true
        }
        if (topicName.length >= 250) {
            return true
        }
        return !regex.matches(topicName)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleStoreProcessor::class.java)
    }
}
