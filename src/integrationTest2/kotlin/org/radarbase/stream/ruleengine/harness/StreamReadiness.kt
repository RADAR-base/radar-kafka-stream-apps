package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.streams.KafkaStreams
import java.time.Duration
import java.time.Instant

object StreamReadiness {
    private val POLL_INTERVAL: Duration = Duration.ofMillis(100)
    private val TERMINAL_STATES = setOf(KafkaStreams.State.ERROR, KafkaStreams.State.PENDING_SHUTDOWN, KafkaStreams.State.NOT_RUNNING)

    fun awaitRunning(streams: List<KafkaStreams>, timeout: Duration = Duration.ofSeconds(30)) {
        streams.forEach { awaitRunning(it, timeout) }
    }

    /** Fails fast when [streams] has crashed or shut down, so waiters don't sit out their full timeout. */
    fun checkNotTerminal(streams: KafkaStreams, waitingFor: String) {
        val state = streams.state()
        check(state !in TERMINAL_STATES) { "KafkaStreams entered terminal state $state while waiting for $waitingFor" }
    }

    private fun awaitRunning(streams: KafkaStreams, timeout: Duration) {
        val deadline = Instant.now().plus(timeout)
        while (streams.state() != KafkaStreams.State.RUNNING) {
            checkNotTerminal(streams, "RUNNING")
            check(Instant.now().isBefore(deadline)) {
                "KafkaStreams did not reach RUNNING within $timeout (state=${streams.state()})"
            }
            Thread.sleep(POLL_INTERVAL.toMillis())
        }
    }
}
