package org.radarbase.stream

import org.radarbase.config.GlobalStoreConfig
import org.radarbase.topic.KafkaTopic

/**
 * Defines a single Kafka stream processing step.
 * It maps an input topic to an output topic, optionally with a global store.
 */
class StreamDefinition(
    val inputTopic: KafkaTopic,
    val outputTopic: KafkaTopic,
    val globalStoreConfig: GlobalStoreConfig? = null,
)
