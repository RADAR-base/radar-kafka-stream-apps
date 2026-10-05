package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.streams.KafkaStreams
import java.time.Duration
import java.time.Instant
import java.util.Properties

object OutputTopicConsumer {
    private val POLL_INTERVAL: Duration = Duration.ofMillis(200)

    fun <V> waitForRecordValues(
        bootstrapServers: String,
        topic: String,
        minCount: Int,
        timeout: Duration = Duration.ofSeconds(10),
        streams: KafkaStreams? = null,
        decodeValue: (ByteArray) -> V,
    ): List<V> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "it-$topic-${System.nanoTime()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        KafkaConsumer(props, ByteArrayDeserializer(), ByteArrayDeserializer()).use { consumer ->
            consumer.subscribe(listOf(topic))
            val values = mutableListOf<V>()
            val deadline = Instant.now().plus(timeout)
            while (values.size < minCount && Instant.now().isBefore(deadline)) {
                streams?.let { StreamReadiness.checkNotTerminal(it, "records on $topic") }
                val records = consumer.poll(POLL_INTERVAL)
                records.forEach { record -> values += decodeValue(record.value()) }
            }
            return values
        }
    }
}
