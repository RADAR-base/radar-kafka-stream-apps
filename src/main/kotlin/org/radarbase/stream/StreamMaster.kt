package org.radarbase.stream

import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.StreamApplicationsProcess
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Orchestrates the lifecycle of multiple [StreamWorker]s.
 * It handles starting, stopping, and monitoring of all configured streams in a single process,
 * ensuring they are managed as a cohesive unit.
 */
open class StreamMaster(
    propertyHandler: RadarConfigHandler,
    streams: Stream<out SingleStreamConfig>,
) : StreamApplicationsProcess, Thread.UncaughtExceptionHandler {
    private val streamWorkers: List<StreamWorker>
    private val currentStream = AtomicInteger(0)
    private lateinit var executor: ExecutorService

    init {
        streamWorkers = streams
            .map { createWorker(propertyHandler, it) }
            .toList()
        logger.info(
            "Configured streams: \n{}",
            streamWorkers.joinToString("\n") { " - ${it.javaClass.name}" },
        )
    }

    private fun createWorker(config: RadarConfigHandler, c: SingleStreamConfig): StreamWorker {
        return try {
            val worker = c.streamClass!!.getDeclaredConstructor().newInstance() as StreamWorker
            worker.configure(this, config, c)
            worker
        } catch (e: Exception) {
            when (e) {
                is InstantiationException,
                is IllegalAccessException,
                is NoSuchMethodException,
                -> throw IllegalArgumentException(
                    "Cannot instantiate class ${c.streamClass}",
                    e,
                )

                is ClassCastException -> throw IllegalArgumentException(
                    "Given class ${c.streamClass} does not implement StreamWorker.",
                    e,
                )

                else -> throw e
            }
        }
    }

    @Throws(IOException::class)
    override fun start() {
        executor = Executors.newSingleThreadExecutor()
        executor.execute { Thread.currentThread().uncaughtExceptionHandler = this }

        announceTopics()

        logger.info("Starting all streams")

        val exs = streamWorkers.map { worker ->
            executor.submit { worker.start() }
        }.mapNotNull { future ->
            try {
                future.get()
                null
            } catch (e: Exception) {
                e
            }
        }

        if (exs.isNotEmpty()) {
            exs.forEach { logger.error("Failed to start stream", it) }
            throw IOException("Failed to start streams", exs[0])
        }
    }

    @Throws(InterruptedException::class)
    override fun shutdown() {
        if (executor.isShutdown) {
            logger.warn("Streams already shut down, will not shut down again.")
            return
        }
        logger.info("Shutting down all streams")

        streamWorkers.forEach { worker -> executor.execute { worker.shutdown() } }
        executor.shutdown()
        executor.awaitTermination(30, TimeUnit.SECONDS)
    }

    fun notifyStartedStream(stream: StreamWorker) {
        val current = currentStream.incrementAndGet()
        logger.info(
            "{} is started. {}/{} streams are now running",
            stream,
            current,
            streamWorkers.size,
        )
    }

    fun notifyClosedStream(stream: StreamWorker) {
        val current = currentStream.decrementAndGet()
        if (current == 0) {
            logger.info("{} is closed. All streams have been terminated", stream)
        } else {
            logger.info(
                "{} is closed. {}/{} streams are still running",
                stream,
                current,
                streamWorkers.size,
            )
        }
    }

    fun notifyCrashedStream(stream: String) {
        logger.error("{} is crashed", stream)
        logger.info("Forcing shutdown of {}", stream)
        try {
            shutdown()
        } catch (ex: InterruptedException) {
            logger.warn("Shutdown interrupted: {}", ex.message)
        }
    }

    protected fun announceTopics() {
        logger.info(
            "If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics before starting: \n  - {}",
            streamWorkers.asSequence().flatMap { it.getStreamDefinitions().collect(Collectors.toList()) }
                .flatMap { listOfNotNull(it.inputTopic, it.outputTopic) }.map { it.name }.distinct().sorted()
                .joinToString("\n - "),
        )
    }
    override fun uncaughtException(t: Thread, e: Throwable) {
        logger.error("StreamMaster error in Thread {}", t.name, e)
        try {
            shutdown()
        } catch (_: InterruptedException) {
            // ignore
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StreamMaster::class.java)
    }
}
