package com.example.kafkaoutbox.listener;

import com.example.kafkaoutbox.application.InputEventService;
import com.example.kafkaoutbox.application.KafkaHeaderCopier;
import com.example.kafkaoutbox.application.PublishMode;
import com.example.kafkaoutbox.application.ProcessingDecision;
import com.example.kafkaoutbox.config.AppKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InputEventListener {

    private static final Logger log = LoggerFactory.getLogger(InputEventListener.class);

    private final InputEventService service;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final AppKafkaProperties kafkaProperties;

    public InputEventListener(InputEventService service,
                              @Qualifier("processedKafkaTemplate") KafkaTemplate<String, byte[]> kafkaTemplate,
                              AppKafkaProperties kafkaProperties) {
        this.service = service;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @KafkaListener(topics = "${app.kafka.input-topic}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, byte[]> record, Acknowledgment ack) throws Exception {
        ProcessingDecision decision = service.process(record);

        if (decision.publishMode() == PublishMode.DIRECT_PUBLISH) {
            ProducerRecord<String, byte[]> output = new ProducerRecord<>(
                    kafkaProperties.processedTopic(),
                    record.key(),
                    decision.plainOutputPayload()
            );
            KafkaHeaderCopier.copyInputHeadersAndCurrentCorrelationId(record, output);
            kafkaTemplate.send(output).get();
            log.info("Direct published processed Avro payload to processed topic with encrypting producer.");
        }

        ack.acknowledge();
    }
}
