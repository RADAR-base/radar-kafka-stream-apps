package org.radarbase.stream.ruleengine.harness

import org.radarbase.stream.AbstractStreamWorker
import java.time.Duration

object StreamTeardown {
    private val CLOSE_TIMEOUT: Duration = Duration.ofSeconds(5)

    fun closeAll(workers: List<AbstractStreamWorker>, timeout: Duration = CLOSE_TIMEOUT) {
        workers.forEach { worker ->
            worker.kafkaStreamsInstances?.forEach { streams ->
                if (!streams.close(timeout)) {
                    System.err.println("KafkaStreams did not close within $timeout (state=${streams.state()}); continuing teardown")
                }
            }
        }
    }
}
