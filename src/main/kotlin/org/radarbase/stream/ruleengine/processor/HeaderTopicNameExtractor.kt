package org.radarbase.stream.ruleengine.processor

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.RecordContext
import org.apache.kafka.streams.processor.TopicNameExtractor
import org.radarbase.stream.ruleengine.domain.Action

class HeaderTopicNameExtractor : TopicNameExtractor<GenericRecord, Action> {
    override fun extract(key: GenericRecord?, action: Action, recordContext: RecordContext): String {
        val header = checkNotNull(recordContext.headers().lastHeader(RuleEvaluationProcessor.TARGET_TOPIC_HEADER)) {
            "Record has no ${RuleEvaluationProcessor.TARGET_TOPIC_HEADER} header"
        }
        return header.value().toString(Charsets.UTF_8)
    }
}
