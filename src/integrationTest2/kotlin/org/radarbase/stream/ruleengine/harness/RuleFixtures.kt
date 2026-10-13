package org.radarbase.stream.ruleengine.harness

import org.radarbase.stream.ruleengine.domain.Action
import org.radarbase.stream.ruleengine.domain.Condition
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.radarbase.stream.ruleengine.domain.Rule

object RuleFixtures {
    fun rule(
        name: String,
        expression: String,
        outputTopic: String,
        actionName: String = "notify",
        scope: String,
    ) = Rule(
        name = name,
        scope = scope,
        condition = Condition(type = ExpressionType.CEL, expression = expression, name = "${name}_condition"),
        action = Action(topic = outputTopic, name = actionName),
    )
}
