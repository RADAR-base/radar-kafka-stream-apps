package org.radarbase.stream.ruleengine.evaluator

import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType

/**
 * Factory class for [ConditionEvaluator]s. It instantiates conditions based on the configuration
 * provided for the given consumer.
 */
object ConditionFactory {
    fun getConditionEvaluator(condition: Condition): CelConditionEvaluator {
        return when (condition.type) {
            CelConditionEvaluator.TYPE_NAME -> CelConditionEvaluator
            else -> throw IllegalArgumentException(
                "The specified condition with type " + condition.type + " is not correct. Implementations are: " + ExpressionType.entries.joinToString(
                    ", ",
                ) { it.name },
            )
        }
    }
}
