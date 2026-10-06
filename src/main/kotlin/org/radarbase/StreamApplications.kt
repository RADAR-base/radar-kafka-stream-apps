package org.radarbase

import org.apache.commons.cli.ParseException
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.StreamApplicationsCliOptions
import org.radarbase.config.StreamApplicationsProcess
import org.radarbase.stream.KafkaStreamFactory
import org.radarbase.util.RadarSingletonFactory
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.system.exitProcess

/**
 * Core class that initializes configurations and then start configured Kafka streams
 */
class StreamApplications(
    private val cliOptions: StreamApplicationsCliOptions,
    private val radarConfigHandler: RadarConfigHandler = createPropertyHandler(cliOptions),
) {

    private lateinit var process: StreamApplicationsProcess

    init {
        logger.info("Configuration successfully updated")
        logger.info("radar.yml configuration: {}", radarConfigHandler.radarProperties)
    }

    /**
     * Starts streams and sets a ShutdownHook to close streams while closing the application
     */
    fun run() {
        try {
            start()
        } catch (ex: IOException) {
            logger.error("FATAL ERROR! The current instance cannot start", ex)
            exitProcess(1)
        } catch (ex: InterruptedException) {
            logger.error("The current instance was interrupted", ex)
        }

        Runtime.getRuntime().addShutdownHook(
            Thread {
                try {
                    shutdown()
                } catch (ex: Exception) {
                    logger.error("Impossible to finalise the shutdown hook", ex)
                }
            },
        )
    }

    /**
     * Start here all needed StreamMasters
     *
     * @throws IOException if the command failed to start up
     * @throws InterruptedException if the command was interrupted
     */
    @Throws(IOException::class, InterruptedException::class)
    fun start() {
        logger.info("STARTING")

        process = createProcess()
        process.start()

        logger.info("STARTED")
    }

    /**
     * Stop here all commands started inside [start].
     *
     * @throws IOException if the command failed to shut down
     * @throws InterruptedException if the command was interrupted before completely shutting down
     */
    @Throws(InterruptedException::class, IOException::class)
    fun shutdown() {
        logger.info("SHUTTING DOWN")

        process.shutdown()

        logger.info("FINISHED")
    }

    @Throws(IOException::class)
    fun createProcess(): StreamApplicationsProcess {
        val cliSubCommand = cliOptions.subCommand ?: "stream"
        return when (cliSubCommand) {
            "stream" -> KafkaStreamFactory(cliOptions, radarConfigHandler).createSensorStreams()
            else -> throw IllegalArgumentException("Unknown subcommand ${cliOptions.subCommand}")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StreamApplications::class.java)

        @Throws(IOException::class)
        private fun createPropertyHandler(options: StreamApplicationsCliOptions): RadarConfigHandler {
            val properties = RadarSingletonFactory.radarConfigHandler
            properties.load(options.propertyPath)
            return properties
        }

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val options = StreamApplicationsCliOptions.parse(args)
                val streamApplications = StreamApplications(options)
                streamApplications.run()
            } catch (_: ParseException) {
                logger.error(
                    "Cannot parse arguments {}. Valid options are:\n{}",
                    args.contentToString(),
                    StreamApplicationsCliOptions.OPTIONS,
                )
                exitProcess(1)
            } catch (ex: Exception) {
                logger.error("Failed to run command", ex)
                exitProcess(1)
            }
        }
    }
}
