package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.ConditionConfig

data class InterventionConfig(
    val name: String?,
    val topic: String,
    @JsonProperty("conditions")
    val conditionConfigs: List<ConditionConfig>,
    @JsonProperty("actions")
    val actionConfigs: List<ActionConfig>,
    @JsonProperty("consumer_properties")
    val consumerProperties: Map<String, String>? = null,
)