package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.Scoped
import org.slf4j.LoggerFactory

class RuleStoreProcessor(
    val globalStoreName: String,
) : Processor<RuleKey, Rule, Void, Void> {
    private val regex = Regex("^[a-zA-Z0-9._-]*$")
    private lateinit var store: KeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<Void, Void>) {
        store = context.getStateStore(globalStoreName)
    }

    // TODO: Investigate if tumbstone works correctly. -> Check what event the database sends.
    override fun process(record: Record<RuleKey, Rule>) {
        val key = record.key() ?: return
        val rule = record.value()
        val storeKey = key.toStoreKey()

        val newScopeKeys = rule
            ?.conditions
            ?.flatMapTo(mutableSetOf()) { it.toScopeKeys() }
            .orEmpty()

        removeStaleMemberships(storeKey, newScopeKeys)

        if (rule == null) {
            logger.info("Received tombstone for rule key {}; removed from all scopes", key)
            return
        }
        if (!isValidTopicName(rule.topic)) {
            logger.info("Received invalid topic name {}", rule.topic)
            return
        }

        rule.conditions.forEach { condition ->

            logger.debug("Processing condition {} for rule {}", condition, key)

            condition.toScopeKeys().forEach { scopeKey ->
                val currentGroup = store.get(scopeKey) ?: RuleGroup()
                val groupRules = currentGroup.rules + (storeKey to rule)
                store.put(scopeKey, RuleGroup(groupRules))
                logger.debug("Added condition key {} for scope {}", key, scopeKey)
            }
        }
    }

    private fun isValidTopicName(topicName: String): Boolean {
        if (topicName.isBlank()) {
            return false
        }
        if (topicName.length >= 250) {
            return false
        }
        return regex.matches(topicName)
    }

    private fun removeStaleMemberships(storeKey: String, newScopeKeys: Set<String>) {
        val staleScopeKeys = store.all().use { iterator ->
            iterator.asSequence()
                .filter { it.key !in newScopeKeys && it.value.rules.containsKey(storeKey) }
                .map { it.key }
                .toList()
        }

        staleScopeKeys.forEach { scopeKey ->
            val group = store.get(scopeKey) ?: return@forEach
            val remainingRules = group.rules.toMutableMap().apply { remove(storeKey) }
            if (remainingRules.isEmpty()) {
                store.delete(scopeKey)
            } else {
                store.put(scopeKey, RuleGroup(remainingRules))
            }
            logger.debug("Removed stale rule key {} from scope {}", storeKey, scopeKey)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleStoreProcessor::class.java)
    }
}

fun Scoped.toScopeKeys(): List<String> {
    val subjects = this.subjects
    if (!subjects.isNullOrEmpty()) {
        return subjects.map { "${ScopePrefix.USER.scope}$it" }
    }
    val projects = this.projects
    if (!projects.isNullOrEmpty()) {
        return projects.map { "${ScopePrefix.PROJECT.scope}$it" }
    }
    return listOf(ScopePrefix.GLOBAL.scope)
}

enum class ScopePrefix(val scope: String) {
    GLOBAL("global"), PROJECT("project."), USER("user."),
}
