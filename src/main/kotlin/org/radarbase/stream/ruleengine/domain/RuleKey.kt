package org.radarbase.stream.ruleengine.domain

// TODO make the key agnostic for AppConfig. Ask Pim what this means
data class RuleKey(
    val scope: String,
    val name: String,
) {
    fun toStoreKey(): String = "$scope|$name"
}
