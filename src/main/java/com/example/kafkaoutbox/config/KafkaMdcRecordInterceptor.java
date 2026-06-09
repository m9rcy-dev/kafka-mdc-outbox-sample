package com.example.kafkaoutbox.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

@Component
public class KafkaMdcRecordInterceptor implements RecordInterceptor<String, byte[]> {

    public static final String CORRELATION_ID = "correlationId";

    @Override
    public ConsumerRecord<String, byte[]> intercept(ConsumerRecord<String, byte[]> record,
                                                    Consumer<String, byte[]> consumer) {
        String correlationId = Optional.ofNullable(lastHeader(record, CORRELATION_ID))
                .filter(value -> !value.isBlank())
                .orElseGet(() -> UUID.randomUUID().toString());

        MDC.put(CORRELATION_ID, correlationId);
        MDC.put("kafkaTopic", record.topic());
        MDC.put("kafkaPartition", String.valueOf(record.partition()));
        MDC.put("kafkaOffset", String.valueOf(record.offset()));
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<String, byte[]> record, Consumer<String, byte[]> consumer) {
        MDC.clear();
    }

    @Override
    public void failure(ConsumerRecord<String, byte[]> record, Exception exception, Consumer<String, byte[]> consumer) {
        MDC.clear();
    }

    private String lastHeader(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
