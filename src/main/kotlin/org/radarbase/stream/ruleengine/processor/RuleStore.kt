package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.state.KeyValueStore
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.ScopePrefix

class RuleStore(val store: KeyValueStore<String, RuleGroup>) {

    fun remove(ruleKey: RuleKey) {
        val group = store[ruleKey.scope]
        if (group != null) {
            store.put(
                ruleKey.scope,
                group.copy(
                    rules = group.rules - ruleKey.name,
                ),
            )
        }
    }
    fun add(ruleKey: RuleKey, rule: Rule) {
        val group = store[ruleKey.scope].let {
            when (it) {
                null -> RuleGroup()
                else -> it
            }
        }

        store.put(
            ruleKey.scope,
            group.copy(
                rules = group.rules + (ruleKey.name to rule),
            ),
        )
    }

    fun getRules(user: String, project: String): List<Rule> {
        val userScope = store.get(ScopePrefix.USER.prefix + user)?.rules.orEmpty()
        val projectScope = store.get(ScopePrefix.PROJECT.prefix + project)?.rules.orEmpty()
        val globalScope = store.get(ScopePrefix.GLOBAL.prefix)?.rules.orEmpty()
        val overrideByProjectScope = globalScope + projectScope
        val overrideByUserScope = overrideByProjectScope + userScope
        return overrideByUserScope.map { it.value }.toList()
    }
}
