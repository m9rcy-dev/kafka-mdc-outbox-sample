package com.example.kafkaoutbox.application;

import com.example.kafkaoutbox.avro.AvroEventReader;
import com.example.kafkaoutbox.avro.AvroEventWriter;
import com.example.kafkaoutbox.common.MissingBusinessRecordException;
import com.example.kafkaoutbox.config.AppKafkaProperties;
import com.example.kafkaoutbox.crypto.LocalPayloadCrypto;
import com.example.kafkaoutbox.domain.OutputEvent;
import com.example.kafkaoutbox.outbox.OutboxEventRepository;
import com.example.kafkaoutbox.record.BusinessRecordRepository;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InputEventServiceTest {

    @Test
    void nonNewEventForMissingRecordIsRetriedInsteadOfPublished() {
        BusinessRecordRepository businessRecords = mock(BusinessRecordRepository.class);
        OutboxEventRepository outboxEvents = mock(OutboxEventRepository.class);
        LocalPayloadCrypto crypto = new LocalPayloadCrypto();
        InputEventService service = new InputEventService(
                businessRecords,
                outboxEvents,
                new AvroEventReader(),
                new AvroEventWriter(),
                crypto,
                new AppKafkaProperties("input_topic", "processed_topic", ".DLQ")
        );
        when(businessRecords.existsByRecordId("R1")).thenReturn(false);
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "input_topic",
                0,
                12L,
                "R1",
                crypto.encrypt(avroInputBytes("R1", "UPDATE", "Second"))
        );

        assertThatThrownBy(() -> service.process(record))
                .isInstanceOf(MissingBusinessRecordException.class)
                .hasMessageContaining("R1");

        verify(outboxEvents, never()).save(any());
    }

    private byte[] avroInputBytes(String recordId, String eventType, String name) {
        Schema schema = loadSchema("/avro/input-event.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("eventType", new GenericData.EnumSymbol(schema.getField("eventType").schema(), eventType));
        record.put("name", name);
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private Schema loadSchema(String path) {
        try (InputStream inputStream = getClass().getResourceAsStream(path)) {
            return new Schema.Parser().parse(Objects.requireNonNull(inputStream, "Missing schema " + path));
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
