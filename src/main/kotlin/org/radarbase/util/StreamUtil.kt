package org.radarbase.util

import org.apache.kafka.streams.StreamsConfig
import org.radarbase.config.KafkaProperty
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.StreamApplicationsConfig
import java.time.Duration
import java.util.*
import java.util.concurrent.ThreadLocalRandom

private val COMMIT_INTERVAL: Duration = Duration.ofSeconds(30)

internal fun getStreamProperties(
    clazz: Class<*>,
    streamConfig: SingleStreamConfig,
    config: StreamApplicationsConfig,
    kafkaProperty: KafkaProperty,
): Properties {
    val localClientId = "${clazz.name}-${config.buildVersion}"
    val props = kafkaProperty.getStreamProperties(localClientId, streamConfig)

    val interval = (ThreadLocalRandom.current().nextDouble(0.75, 1.25) * COMMIT_INTERVAL.toMillis()).toLong()

    props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = interval.toString()

    return props
}
