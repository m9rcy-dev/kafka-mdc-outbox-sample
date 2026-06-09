package com.example.kafkaoutbox;

import com.example.kafkaoutbox.avro.AvroEventReader;
import com.example.kafkaoutbox.avro.AvroEventWriter;
import com.example.kafkaoutbox.common.BadInputException;
import com.example.kafkaoutbox.crypto.EncryptingByteArraySerializer;
import com.example.kafkaoutbox.crypto.LocalPayloadCrypto;
import com.example.kafkaoutbox.domain.EventType;
import com.example.kafkaoutbox.domain.InputEvent;
import com.example.kafkaoutbox.domain.OutputEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaMdcOutboxSampleApplicationTests {

    @Test
    void localCryptoDecryptsEncryptedInputAndEncryptsProcessedOutput() {
        LocalPayloadCrypto crypto = new LocalPayloadCrypto();
        byte[] plain = avroInputBytes("R1", "NEW", "First");

        byte[] encrypted = crypto.encrypt(plain);

        assertThat(new String(encrypted, StandardCharsets.UTF_8)).startsWith("enc:");
        assertThat(crypto.decrypt(encrypted)).isEqualTo(plain);
    }

    @Test
    void localCryptoRejectsPlainInput() {
        LocalPayloadCrypto crypto = new LocalPayloadCrypto();

        assertThatThrownBy(() -> crypto.decrypt("{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(BadInputException.class)
                .hasMessageContaining("not encrypted");
    }

    @Test
    void encryptingSerializerEncryptsProducerPayloads() {
        LocalPayloadCrypto crypto = new LocalPayloadCrypto();
        EncryptingByteArraySerializer serializer = new EncryptingByteArraySerializer(crypto);
        byte[] plainOutput = "{\"recordId\":\"R1\"}".getBytes(StandardCharsets.UTF_8);

        byte[] serialized = serializer.serialize("processed_topic", plainOutput);

        assertThat(new String(serialized, StandardCharsets.UTF_8)).startsWith("enc:");
        assertThat(crypto.decrypt(serialized)).isEqualTo(plainOutput);
    }

    @Test
    void avroStandInsReadPlainInputAndWritePlainOutput() {
        AvroEventReader reader = new AvroEventReader();
        AvroEventWriter writer = new AvroEventWriter();
        byte[] plainInput = avroInputBytes("R1", "UPDATE", "Second");

        InputEvent inputEvent = reader.read(plainInput, new RecordHeaders());
        byte[] plainOutput = writer.write(OutputEvent.from(inputEvent));
        GenericRecord outputRecord = readAvro("/avro/processed-event.avsc", plainOutput);

        assertThat(inputEvent.recordId()).isEqualTo("R1");
        assertThat(inputEvent.eventType()).isEqualTo(EventType.UPDATE);
        assertThat(outputRecord.get("recordId").toString()).isEqualTo("R1");
        assertThat(outputRecord.get("eventType").toString()).isEqualTo("UPDATE");
        assertThat(outputRecord.get("processingStatus").toString()).isEqualTo("ENRICHED");
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

    private GenericRecord readAvro(String schemaPath, byte[] bytes) {
        Schema schema = loadSchema(schemaPath);
        try {
            return new GenericDatumReader<GenericRecord>(schema)
                    .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
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
