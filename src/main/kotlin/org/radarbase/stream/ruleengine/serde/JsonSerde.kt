package org.radarbase.stream.ruleengine.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class JsonSerde<T : Any>(private val clazz: Class<T>) : Serde<T> {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    override fun serializer(): Serializer<T> = Serializer { _, data ->
        data?.let { mapper.writeValueAsBytes(it) }
    }

    override fun deserializer(): Deserializer<T> = Deserializer { _, data ->
        data?.let { mapper.readValue(it, clazz) }
    }
}
