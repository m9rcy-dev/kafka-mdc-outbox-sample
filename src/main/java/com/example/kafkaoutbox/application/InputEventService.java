package com.example.kafkaoutbox.application;

import com.example.kafkaoutbox.avro.AvroEventReader;
import com.example.kafkaoutbox.avro.AvroEventWriter;
import com.example.kafkaoutbox.common.MissingBusinessRecordException;
import com.example.kafkaoutbox.config.AppKafkaProperties;
import com.example.kafkaoutbox.crypto.PayloadCrypto;
import com.example.kafkaoutbox.domain.EventType;
import com.example.kafkaoutbox.domain.InputEvent;
import com.example.kafkaoutbox.domain.OutputEvent;
import com.example.kafkaoutbox.outbox.OutboxEvent;
import com.example.kafkaoutbox.outbox.OutboxEventRepository;
import com.example.kafkaoutbox.outbox.OutboxStatus;
import com.example.kafkaoutbox.record.BusinessRecord;
import com.example.kafkaoutbox.record.BusinessRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class InputEventService {

    private static final Logger log = LoggerFactory.getLogger(InputEventService.class);

    private final BusinessRecordRepository businessRecordRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AvroEventReader avroEventReader;
    private final AvroEventWriter avroEventWriter;
    private final PayloadCrypto payloadCrypto;
    private final AppKafkaProperties kafkaProperties;

    public InputEventService(BusinessRecordRepository businessRecordRepository,
                             OutboxEventRepository outboxEventRepository,
                             AvroEventReader avroEventReader,
                             AvroEventWriter avroEventWriter,
                             PayloadCrypto payloadCrypto,
                             AppKafkaProperties kafkaProperties) {
        this.businessRecordRepository = businessRecordRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.avroEventReader = avroEventReader;
        this.avroEventWriter = avroEventWriter;
        this.payloadCrypto = payloadCrypto;
        this.kafkaProperties = kafkaProperties;
    }

    @Transactional
    public ProcessingDecision process(ConsumerRecord<String, byte[]> record) {
        byte[] plainInputPayload = payloadCrypto.decrypt(record.value());
        InputEvent event = avroEventReader.read(plainInputPayload, record.headers());
        byte[] plainOutputPayload = avroEventWriter.write(OutputEvent.from(event));

        MDC.put("recordId", event.recordId());
        MDC.put("eventType", event.eventType().name());

        boolean exists = businessRecordRepository.existsByRecordId(event.recordId());
        boolean hasPendingOutbox = outboxEventRepository.existsByRecordIdAndStatusIn(
                event.recordId(),
                List.of(OutboxStatus.PENDING)
        );

        if (!exists && event.eventType() == EventType.NEW) {
            log.info("Record does not exist. Enriching, saving DB record, then outboxing plain output payload.");
            businessRecordRepository.save(BusinessRecord.fromNewEvent(event));
            saveOutbox(record, event.recordId(), plainOutputPayload);
            return ProcessingDecision.outboxed();
        }

        if (!exists) {
            log.warn("Record does not exist yet for non-NEW event. Retrying instead of publishing.");
            throw new MissingBusinessRecordException(event.recordId());
        }

        if (hasPendingOutbox) {
            log.info("Record has pending outbox. Outboxing this event too to prevent overtaking.");
            saveOutbox(record, event.recordId(), plainOutputPayload);
            return ProcessingDecision.outboxed();
        }

        log.info("Record exists and no pending outbox. Direct publish is safe.");
        return ProcessingDecision.directPublish(plainOutputPayload);
    }

    private void saveOutbox(ConsumerRecord<String, byte[]> record, String recordId, byte[] plainOutputPayload) {
        outboxEventRepository.save(OutboxEvent.from(
                record,
                recordId,
                plainOutputPayload,
                MDC.get("correlationId"),
                kafkaProperties
        ));
    }
}
