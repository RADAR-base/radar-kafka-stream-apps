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
            ExpressionType.CEL -> CelConditionEvaluator
        }
    }
}
