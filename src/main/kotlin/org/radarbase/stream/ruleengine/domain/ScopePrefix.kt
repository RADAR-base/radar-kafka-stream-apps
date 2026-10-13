package org.radarbase.stream.ruleengine.domain

enum class ScopePrefix(val prefix: String) {
    GLOBAL("global"), PROJECT("project."), USER("user."),
}
