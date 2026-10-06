package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import org.radarbase.config.ConfigUtils.Companion.withEnvVars
import java.util.*

class StreamConfig {

    @JsonIgnore
    private val priorityThreads: MutableMap<RadarConfigHandler.Priority, Int> =
        EnumMap(RadarConfigHandler.Priority::class.java)

    @JsonProperty
    var properties: Map<String, String>? = null
        get() = (field ?: emptyMap()).withEnvVars(ENV_VAR_PREFIX_PROPERTIES)

    @JsonProperty("streams")
    var streamConfigs: List<SingleStreamConfig>? = null

    init {
        priorityThreads[RadarConfigHandler.Priority.LOW] = 1
        priorityThreads[RadarConfigHandler.Priority.NORMAL] = 2
        priorityThreads[RadarConfigHandler.Priority.HIGH] = 4
    }

    @get:JsonGetter("threads_per_priority")
    var threadsPerPriority: Map<String, Int>
        get() = priorityThreads.entries.associate { it.key.param to it.value }

        @JsonSetter("threads_per_priority")
        set(streamPriority) {
            streamPriority.values.forEach { v ->
                require(v >= 1) { "Stream priorities cannot be smaller than 1" }
            }
            priorityThreads.putAll(
                streamPriority.entries.associate { (key, value) ->
                    RadarConfigHandler.Priority.valueOf(
                        key.uppercase(Locale.US),
                    ) to value
                },
            )
        }

    fun threadsByPriority(level: RadarConfigHandler.Priority): Int = priorityThreads[level] ?: 1

    companion object {
        const val ENV_VAR_PREFIX_PROPERTIES = "STREAM_"
    }
}
