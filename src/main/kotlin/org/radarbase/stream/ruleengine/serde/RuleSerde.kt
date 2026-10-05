package org.radarbase.stream.ruleengine.serde

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.radarbase.stream.ruleengine.domain.Rule
import org.slf4j.LoggerFactory

// TODO: Should the file avro or schema repo be used?
private val VALUE_SCHEMA: Schema = Schema.Parser().parse(
    """
    {
      "type": "record",
      "name": "AppConfigRow",
      "namespace": "org.radarbase.stream.ruleengine.avro",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "client_id", "type": ["null", "string"], "default": null},
        {"name": "scope", "type": ["null", "string"], "default": null},
        {"name": "name", "type": ["null", "string"], "default": null},
        {"name": "value", "type": ["null", "string"], "default": null},
        {"name": "create_timestamp", "type": ["null", "long"], "default": null},
        {"name": "version", "type": ["null", "int"], "default": null}
      ]
    }
    """.trimIndent(),
)

class RuleSerde : Serde<Rule> {
    private val avroSerde = AvroSerde(VALUE_SCHEMA)
    private val mapper = jacksonObjectMapper()

    override fun serializer(): Serializer<Rule> = Serializer { topic, _ -> avroSerde.unsupportedProducer(topic) }

    override fun deserializer(): Deserializer<Rule> = object : Deserializer<Rule> {
        override fun deserialize(topic: String?, data: ByteArray?): Rule? {
            val bytes = data ?: return null
            val record = avroSerde.decode(bytes)
            val json = record["value"]?.toString()
            if (json == null) {
                logger.debug(
                    "rule_engine_config row id={} client_id={} scope={} name={} has no 'value' payload, treating as tombstone",
                    record["id"],
                    record["client_id"],
                    record["scope"],
                    record["name"],
                )
                return null
            }
            return try {
                mapper.readValue(json, Rule::class.java)
            } catch (e: JsonProcessingException) {
                logger.warn(
                    "Failed to parse Rule JSON for rule_engine_config row id={} client_id={} scope={} name={}: {}",
                    record["id"],
                    record["client_id"],
                    record["scope"],
                    record["name"],
                    e.message,
                )
                null
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleSerde::class.java)
    }
}
