package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

data class Rule(
    val name: String?,
    val topic: String,
    @JsonProperty("conditions")
    val conditions: List<Condition>,
    @JsonProperty("actions")
    val actions: List<Action>,
    @JsonProperty("consumer_properties")
    val consumerProperties: Map<String, String>? = null,
)
