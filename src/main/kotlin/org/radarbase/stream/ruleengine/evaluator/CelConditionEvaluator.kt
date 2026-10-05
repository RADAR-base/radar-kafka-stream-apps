package org.radarbase.stream.ruleengine.evaluator

import dev.cel.common.CelAbstractSyntaxTree
import dev.cel.common.types.CelTypes
import dev.cel.compiler.CelCompiler
import dev.cel.compiler.CelCompilerFactory
import dev.cel.runtime.CelRuntime
import dev.cel.runtime.CelRuntimeFactory
import org.radarbase.stream.ruleengine.domain.ExpressionType
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

object CelConditionEvaluator : ConditionEvaluator {

    val TYPE_NAME = ExpressionType.CEL
    private val logger = LoggerFactory.getLogger(CelConditionEvaluator::class.java)
    const val KEY_MEMBER_NAME = "key"
    const val VALUE_MEMBER_NAME = "value"

    private val celCompiler: CelCompiler =
        CelCompilerFactory.standardCelCompilerBuilder()
            .addVar(KEY_MEMBER_NAME, CelTypes.DYN)
            .addVar(VALUE_MEMBER_NAME, CelTypes.DYN)
            .build()
    private val celRuntime: CelRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build()
    private val astCache = ConcurrentHashMap<String, CelAbstractSyntaxTree>()
    private val programCache = ConcurrentHashMap<String, CelRuntime.Program>()


    override fun isTrueFor(key: Map<String, Any?>, value: Map<String, Any?>, expression: String): Boolean {
        logger.debug("Evaluating expression {} against key: {} and value {}", expression, key, value)

        return getEvaluator(expression).eval(
            mapOf(
                KEY_MEMBER_NAME to key,
                VALUE_MEMBER_NAME to value,
            ),
        ).let {
            it is Boolean && it
        }
    }

    private fun getEvaluator(expression: String): CelRuntime.Program = programCache.computeIfAbsent(expression) { condition ->
        val ast = astCache.computeIfAbsent(condition) {
            logger.debug("Compiling CEL expression: {}", condition)
            celCompiler.compile(it).ast
        }
        celRuntime.createProgram(ast)
    }
}
