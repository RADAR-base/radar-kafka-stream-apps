package org.radarbase.stream

import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.StreamApplicationsCliOptions
import java.util.*
import java.util.stream.Stream

/**
 * Factory for creating [StreamMaster] and [StreamWorker] instances.
 * It uses the configuration file and CLI options to determine which streams should be instantiated and started.
 */
class KafkaStreamFactory(
    private val cliOptions: StreamApplicationsCliOptions,
    private val radarProperties: RadarConfigHandler,
) {
    fun createSensorStreams(): StreamMaster {
        val cliStreamTypes = cliOptions.subCommandArgs.orEmpty()
            .toHashSet().map { it.lowercase(Locale.US) }

        val streamConfigs = radarProperties.radarProperties.stream?.streamConfigs
            // Keep only StreamConfigs enabled via CLI. Use all StreamConfigs if no CLI args are given.
            ?.filter { s ->
                val name = s.streamClass?.name?.lowercase(Locale.US).orEmpty()
                cliStreamTypes.isEmpty() || cliStreamTypes.any { name.endsWith(it) }
            }
            .orEmpty()
            .stream()

        return streamMaster(streamConfigs)
    }

    private fun streamMaster(streamConfigs: Stream<out SingleStreamConfig>) =
        StreamMaster(radarProperties, streamConfigs)
}
