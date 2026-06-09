package com.example.kafkaoutbox.application;

import com.example.kafkaoutbox.common.OutboxPublishException;
import com.example.kafkaoutbox.config.AppOutboxProperties;
import com.example.kafkaoutbox.outbox.KafkaHeaderJsonCodec;
import com.example.kafkaoutbox.outbox.OutboxEvent;
import com.example.kafkaoutbox.outbox.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final AppOutboxProperties properties;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           @Qualifier("processedKafkaTemplate") KafkaTemplate<String, byte[]> kafkaTemplate,
                           AppOutboxProperties properties) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay-ms:1000}")
    @Transactional
    public void publishPending() {
        if (!properties.enabled()) {
            return;
        }

        List<OutboxEvent> pending = outboxEventRepository.findPendingForPublish(properties.batchSize());
        for (OutboxEvent event : pending) {
            publishOne(event);
        }
    }

    private void publishOne(OutboxEvent event) {
        MDC.put("correlationId", event.getCorrelationId());
        MDC.put("recordId", event.getRecordId());
        MDC.put("outboxId", event.getId().toString());

        try {
            ProducerRecord<String, byte[]> output = new ProducerRecord<>(
                    event.getTargetTopic(),
                    event.getEventKey(),
                    event.getPayloadBytes()
            );
            KafkaHeaderJsonCodec.applyTo(output.headers(), event.getHeadersJson());
            if (event.getCorrelationId() != null) {
                output.headers().remove("correlationId");
                output.headers().add("correlationId", event.getCorrelationId().getBytes(StandardCharsets.UTF_8));
            }

            kafkaTemplate.send(output).get();
            event.markPublished();
            log.info("Published outbox event to processed topic with encrypting producer.");
        } catch (Exception ex) {
            event.markFailed(ex);
            throw new OutboxPublishException("Failed to publish outbox event " + event.getId(), ex);
        } finally {
            MDC.clear();
        }
    }
}
