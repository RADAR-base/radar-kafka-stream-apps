package org.radarbase.stream

import org.apache.kafka.streams.kstream.TimeWindows
import org.radarbase.config.GlobalStoreConfig
import org.radarbase.stream.AbstractStreamWorker.Companion.TIME_WINDOW_COMMIT_INTERVAL_DEFAULT
import org.radarbase.topic.KafkaTopic
import java.time.Duration

/**
 * Defines a single Kafka stream processing step.
 * It maps an input topic to an output topic, optionally specifying time windows and a commit interval.
 */
class StreamDefinition(
    val inputTopic: KafkaTopic,
    val outputTopic: KafkaTopic,
    val timeWindows: TimeWindows? = null,
    val commitInterval: Duration = TIME_WINDOW_COMMIT_INTERVAL_DEFAULT,
    val globalStoreConfig: GlobalStoreConfig? = null,
) {

    val stateStoreName: String
        get() = buildString {
            append("From-")
            append(inputTopic.name)
            outputTopic.let {
                append("-To-")
                append(it.name)
            }
        }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StreamDefinition) return false
        return inputTopic == other.inputTopic && outputTopic == other.outputTopic && timeWindows == other.timeWindows
    }

    override fun hashCode(): Int {
        var result = inputTopic.hashCode()
        result = 31 * result + outputTopic.hashCode()
        result = 31 * result + (timeWindows?.hashCode() ?: 0)
        return result
    }
}
