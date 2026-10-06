package org.radarbase.stream.ruleengine.domain

// TODO remove client_id from query
data class RuleKey(
    val scope: String,
    val name: String,
) {
    fun toStoreKey(): String = "$scope|$name"
}
