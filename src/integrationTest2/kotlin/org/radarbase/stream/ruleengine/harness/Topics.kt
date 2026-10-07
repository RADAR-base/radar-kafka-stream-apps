package org.radarbase.stream.ruleengine.harness

import java.util.UUID

data class Topics(
    val inputTopic: String,
    val outputTopic1: String,
    val outputTopic2: String,
    val globalStoreTopic: String,
    val globalStoreName: String,
) {
    companion object {
        fun unique(): Topics {
            val suffix = UUID.randomUUID().toString().replace("-", "")
            return Topics(
                inputTopic = "input_topic_$suffix",
                outputTopic1 = "output_topic_$suffix",
                outputTopic2 = "output_topic2_$suffix",
                globalStoreTopic = "rule_engine_config_$suffix",
                globalStoreName = "JIT_RULES_STORE_$suffix",
            )
        }
    }
    fun all(): List<String> {
        return listOf(inputTopic, outputTopic1, outputTopic2, globalStoreTopic)
    }
}
