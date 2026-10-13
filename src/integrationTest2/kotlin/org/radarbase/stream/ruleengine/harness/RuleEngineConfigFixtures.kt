package org.radarbase.stream.ruleengine.harness

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.radarbase.stream.ruleengine.domain.Rule
import org.radarbase.stream.ruleengine.domain.RuleKey
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Properties

object RuleEngineConfigFixtures {
    private const val WIRE_FORMAT_MAGIC_BYTE = 0
    private const val FIXTURE_SCHEMA_ID = 1

    private val KEY_SCHEMA: Schema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "AppConfigKey",
          "namespace": "org.radarbase.stream.ruleengine.avro",
          "fields": [
            {"name": "scope", "type": ["null", "string"], "default": null},
            {"name": "name", "type": ["null", "string"], "default": null}
          ]
        }
        """.trimIndent(),
    )

    private val VALUE_SCHEMA: Schema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "AppConfigRow",
          "namespace": "org.radarbase.stream.ruleengine.avro",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "scope", "type": ["null", "string"], "default": null},
            {"name": "name", "type": ["null", "string"], "default": null},
            {"name": "value", "type": ["null", "string"], "default": null},
            {"name": "create_timestamp", "type": ["null", "long"], "default": null},
            {"name": "version", "type": ["null", "int"], "default": null}
          ]
        }
        """.trimIndent(),
    )

    private val mapper = jacksonObjectMapper()

    private fun encode(schema: Schema, record: GenericData.Record): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(WIRE_FORMAT_MAGIC_BYTE)
        out.write(ByteBuffer.allocate(4).putInt(FIXTURE_SCHEMA_ID).array())
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        GenericDatumWriter<GenericData.Record>(schema).write(record, encoder)
        encoder.flush()
        return out.toByteArray()
    }

    fun key(ruleKey: RuleKey): ByteArray = encode(
        KEY_SCHEMA,
        GenericData.Record(KEY_SCHEMA).apply {
            put("scope", ruleKey.scope)
            put("name", ruleKey.name)
        },
    )

    fun value(id: Int, key: RuleKey, rule: Rule, scope: String? = key.scope): ByteArray =
        valueRaw(id, key, mapper.writeValueAsString(rule), scope)

    fun valueRaw(id: Int, key: RuleKey, rawJson: String?, scope: String? = key.scope): ByteArray = encode(
        VALUE_SCHEMA,
        GenericData.Record(VALUE_SCHEMA).apply {
            put("id", id)
            put("scope", scope)
            put("name", key.name)
            put("value", rawJson)
            put("create_timestamp", System.currentTimeMillis())
            put("version", 1)
        },
    )

    fun publishRule(bootstrapServers: String, topic: String, id: Int, key: RuleKey, rule: Rule) =
        publish(bootstrapServers, topic, key(key), value(id, key, rule))

    /** Publishes a Kafka tombstone: a record with the rule key and a null value. */
    fun publishTombstone(bootstrapServers: String, topic: String, key: RuleKey) =
        publish(bootstrapServers, topic, key(key), null)

    fun publish(bootstrapServers: String, topic: String, key: ByteArray, value: ByteArray?) {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        }
        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }
}
