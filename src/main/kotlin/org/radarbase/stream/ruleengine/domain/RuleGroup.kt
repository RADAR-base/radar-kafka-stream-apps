package org.radarbase.stream.ruleengine.domain

data class RuleGroup(
    val rules: Map<String, Rule> = mapOf(),
)
