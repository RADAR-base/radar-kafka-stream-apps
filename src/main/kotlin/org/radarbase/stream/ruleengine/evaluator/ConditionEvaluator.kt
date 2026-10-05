package org.radarbase.stream.ruleengine.evaluator

/**
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * rule's actions are emitted.
 */
interface ConditionEvaluator {

    fun isTrueFor(key: Map<String, Any?>, value: Map<String, Any?>, expression: String): Boolean
}
