package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.time.Duration
import java.time.Instant

object StateStoreQueries {
    private val POLL_INTERVAL: Duration = Duration.ofMillis(100)

    fun <V> waitForValue(
        streams: KafkaStreams,
        storeName: String,
        key: String,
        timeout: Duration = Duration.ofSeconds(10),
    ): V? {
        val deadline = Instant.now().plus(timeout)
        while (true) {
            StreamReadiness.checkNotTerminal(streams, "key '$key' in store $storeName")
            val value = try {
                val store: ReadOnlyKeyValueStore<String, V> = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()),
                )
                store.get(key)
            } catch (ex: InvalidStateStoreException) {
                null
            }
            if (value != null || Instant.now().isAfter(deadline)) return value
            Thread.sleep(POLL_INTERVAL.toMillis())
        }
    }
}
