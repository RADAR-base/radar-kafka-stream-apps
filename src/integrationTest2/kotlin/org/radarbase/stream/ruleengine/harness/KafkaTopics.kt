package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import java.util.Properties
import java.util.concurrent.ExecutionException

/** Creates/deletes the topics a test needs against [KafkaBroker], so tests don't leak state into each other. */
object KafkaTopics {
    fun createTopics(bootstrapServers: String, topics: List<String>) {
        Admin.create(adminProperties(bootstrapServers)).use { admin ->
            try {
                admin.createTopics(topics.map { NewTopic(it, 1, 1) }).all().get()
            } catch (ex: ExecutionException) {
                if (ex.cause !is TopicExistsException) throw ex
            }
            Unit
        }
    }

    private fun adminProperties(bootstrapServers: String) = Properties().apply {
        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    }
}
