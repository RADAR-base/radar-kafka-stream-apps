package org.radarbase.stream

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarBackendConfig
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Stream

/**
 * Abstract implementation of [StreamWorker] that provides common logic for managing Kafka Streams.
 * This class handles lifecycle management (start, shutdown), error handling, and defines the structure
 * for configuring one or more streams.
 */
abstract class AbstractStreamWorker : StreamWorker, StreamsUncaughtExceptionHandler {
    internal val streamDefinitions = CopyOnWriteArrayList<StreamDefinition>()
    protected lateinit var config: SingleStreamConfig
    protected lateinit var allConfig: RadarBackendConfig
    protected lateinit var kafkaProperty: KafkaProperty
    protected lateinit var master: StreamMaster
    protected var streams: List<KafkaStreams>? = null

    val kafkaStreamsInstances: List<KafkaStreams>?
        get() = streams

    protected fun defineStream(def: StreamDefinition) {
        streamDefinitions.add(def)
    }

    override fun getStreamDefinitions(): Stream<StreamDefinition> {
        return streamDefinitions.stream()
    }

    override fun configure(
        streamMaster: StreamMaster,
        properties: RadarConfigHandler,
        singleConfig: SingleStreamConfig,
    ) {
        this.kafkaProperty = properties.kafkaProperties
        this.allConfig = properties.radarProperties
        this.config = singleConfig
        this.master = streamMaster
        this.initialize()
    }

    override fun start() {
        assert(streams == null) { "Streams already started. Cannot start them again." }

        streams = createStreams() ?: throw IllegalStateException("Streams are not initialized during start")

        streams?.forEach { stream ->
            stream.setUncaughtExceptionHandler(this as StreamsUncaughtExceptionHandler?)
            stream.start()
        }

        master.notifyStartedStream(this)
    }

    protected abstract fun createStreams(): List<KafkaStreams>?

    override fun shutdown() {
        logger.info("Shutting down {} stream", javaClass.simpleName)
        closeStreams()
        master.notifyClosedStream(this)
    }

    protected fun closeStreams() {
        streams?.forEach { it.close() }
        streams = null
        doCleanup()
    }

    protected abstract fun doCleanup()

    protected abstract fun initialize()

    override fun handle(e: Throwable?): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse {
        return if (e is StreamsException) {
            logger.error("Stream thread of {} terminated, replacing it: {}", javaClass.simpleName, e.message, e)
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
        } else {
            logger.error("Stream thread of {} terminated by a fatal error, shutting down: {}", javaClass.simpleName, e?.message, e)
            Thread({ master.notifyCrashedStream(javaClass.simpleName) }, "${javaClass.simpleName}-crash-handler").start()
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractStreamWorker::class.java)
    }
}
