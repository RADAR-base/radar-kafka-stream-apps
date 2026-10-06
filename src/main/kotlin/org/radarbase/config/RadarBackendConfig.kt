/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty

data class RadarBackendConfig(
    var broker: List<ServerConfig>? = null,

    @param:JsonProperty("schema_registry")
    var schemaRegistry: List<ServerConfig>? = null,

    @param:JsonProperty("stream")
    var stream: StreamConfig? = null,

    @param:JsonProperty("build_version")
    var buildVersion: String? = null,
) {
    val brokerPaths: String
        get() = broker?.let { ServerConfig.getPaths(it) }
            ?: throw IllegalStateException("Kafka 'broker' is not configured")

    val schemaRegistryPaths: String
        get() = schemaRegistry?.let { ServerConfig.getPaths(it) }
            ?: throw IllegalStateException("'schema_registry' is not configured")

    override fun toString(): String = YamlConfigLoader().prettyString(this)
}
