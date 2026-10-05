package org.radarbase.stream.ruleengine.harness

import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarBackendConfig
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.ServerConfig
import org.radarbase.config.StreamConfig

class FixtureRadarConfigHandler(
    bootstrapServers: String,
    schemaRegistryUrl: String = "http://unused-schema-registry:1",
) : RadarConfigHandler {
    override val radarProperties: RadarBackendConfig = run {
        // KafkaContainer.bootstrapServers may or may not carry a "PLAINTEXT://" scheme prefix
        // depending on the Testcontainers version; java.net.URI misparses a bare "host:port" as
        // an opaque URI (scheme="host", no host/port), so split on the last ':' instead.
        val hostPort = bootstrapServers.substringAfter("://")
        val host = hostPort.substringBeforeLast(':')
        val port = hostPort.substringAfterLast(':').toInt()
        RadarBackendConfig(
            broker = listOf(
                ServerConfig().apply {
                    this.host = host
                    this.port = port
                },
            ),
            schemaRegistry = listOf(
                ServerConfig().apply {
                    this.protocol = schemaRegistryUrl.substringBefore("://")
                    this.host = schemaRegistryUrl.substringAfter("://")
                },
            ),
            stream = StreamConfig().apply {
                threadsPerPriority = mapOf(RadarConfigHandler.Priority.NORMAL.param to 1)
                properties = TEST_STREAM_PROPERTIES
            },
            buildVersion = "integrationTest2",
        )
    }

    override val kafkaProperties: KafkaProperty by lazy { KafkaProperty(radarProperties) }

    override fun load(pathFile: String?) {
        // Fixture config is already loaded in-memory; nothing to do.
    }

    override fun isLoaded(): Boolean = true

    companion object {
        val TEST_STREAM_PROPERTIES: Map<String, String> = mapOf(
            "main.consumer.fetch.max.wait.ms" to "100",
            "global.consumer.fetch.max.wait.ms" to "100",
        )
    }
}
