package org.radarbase.stream.ruleengine.domain

data class Rule(
    val name: String?,
    val scope: String = ScopePrefix.GLOBAL.prefix,
    val condition: Condition, // Check how KCL uses this specification. Using links
    val action: Action,
    // val consumerProperties: Map<String, String>? = null,
)
