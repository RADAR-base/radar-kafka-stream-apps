package org.radarbase.stream.ruleengine.evaluator

/**
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * [org.radarbase.stream.ruleengine.evaluator]s can be triggered.
 */
interface ConditionEvaluator {

    fun isTrueFor(key: Map<String, Any?>, value: Map<String, Any?>, expression: String): Boolean
}
