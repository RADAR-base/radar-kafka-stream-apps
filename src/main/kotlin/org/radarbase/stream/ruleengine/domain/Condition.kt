package org.radarbase.stream.ruleengine.domain

data class Condition(
    val type: ExpressionType,
    val expression: String,
    override val name: String,
    override val properties: Map<String, Any>? = null,
    override val projects: List<String>? = null,
    override val subjects: List<String>? = null,
    override val projectIdField: String? = null,
    override val subjectIdField: String? = null,
    override val sourceIdField: String? = null,
    override val timeField: String? = null,
) : Scoped

enum class ExpressionType(val type: String) {
    CEL("cel"),
}
