package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.radarbase.config.SingleStreamConfig
import org.radarbase.stream.StreamMaster
import org.radarbase.stream.ruleengine.RuleEngineStream
import java.nio.file.Path
import java.util.stream.Stream

object RuleEngineWorkers {
    /**
     * Configures and starts a [RuleEngineStream] the way `StreamMaster.createWorker()` does, and returns its
     * [KafkaStreams] instance. The worker is added to [workers] before `start()`, so teardown also closes a worker whose
     * `start()` throws halfway.
     */
    fun start(
        topics: Topics,
        stateDir: Path,
        workers: MutableList<RuleEngineStream>,
        schemaRegistryUrl: String? = null,
    ): KafkaStreams {
        val singleStreamConfig = SingleStreamConfig().apply {
            streamClass = RuleEngineStream::class.java
            properties = mapOf(
                "input_topic" to topics.inputTopic,
                // RuleEngineStream.initialize() requires an output topic, although rules choose their own.
                "output_topic" to topics.outputTopic1,
                "global_store_name" to topics.globalStoreName,
                "global_store_topic" to topics.globalStoreTopic,
                StreamsConfig.STATE_DIR_CONFIG to stateDir.toString(),
                StreamsConfig.APPLICATION_ID_CONFIG to "rule-engine-it-${topics.globalStoreName}",
            )
        }

        val configHandler = if (schemaRegistryUrl == null) {
            FixtureRadarConfigHandler(KafkaBroker.bootstrapServers)
        } else {
            FixtureRadarConfigHandler(KafkaBroker.bootstrapServers, schemaRegistryUrl)
        }
        val master = StreamMaster(configHandler, Stream.empty())
        val worker = RuleEngineStream()
        worker.configure(master, configHandler, singleStreamConfig)
        workers += worker
        worker.start()

        return requireNotNull(worker.kafkaStreamsInstances) { "RuleEngineStream did not build any KafkaStreams instances" }.first()
    }
}
