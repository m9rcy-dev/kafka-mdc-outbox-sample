package com.example.kafkaoutbox.application;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;

public final class KafkaHeaderCopier {

    private KafkaHeaderCopier() {
    }

    public static void copyInputHeadersAndCurrentCorrelationId(ConsumerRecord<String, byte[]> source,
                                                               ProducerRecord<String, byte[]> target) {
        source.headers().forEach(header -> target.headers().add(header.key(), header.value()));
        String correlationId = MDC.get("correlationId");
        if (correlationId != null) {
            target.headers().remove("correlationId");
            target.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
        }
    }
}
