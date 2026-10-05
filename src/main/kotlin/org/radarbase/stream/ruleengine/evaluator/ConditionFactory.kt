package org.radarbase.stream.ruleengine.evaluator

import org.radarbase.stream.ruleengine.domain.ConditionConfig
import org.radarbase.stream.ruleengine.domain.ExpressionType

/**
 * Factory class for [ConditionEvaluator]s. It instantiates conditions based on the configuration
 * provided for the given consumer.
 */
object ConditionFactory {
    @JvmStatic
    fun getConditionEvaluator(conditionConfig: ConditionConfig): CelConditionEvaluator {
        return when (conditionConfig.type) {
            CelConditionEvaluator.TYPE_NAME -> CelConditionEvaluator
            else -> throw IllegalArgumentException(
                "The specified condition with type " + conditionConfig.type + " is not correct. Implementations are: " + ExpressionType.entries.joinToString(
                    ", ",
                ) { it.name },
            )
        }
    }
}
