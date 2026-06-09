package com.example.kafkaoutbox.avro;

import com.example.kafkaoutbox.domain.OutputEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Writes plain processed Avro binary payloads.
 * The processed KafkaTemplate encrypts these bytes during producer serialization.
 */
@Component
public class AvroEventWriter {

    private final Schema schema;
    private final DatumWriter<GenericRecord> datumWriter;

    public AvroEventWriter() {
        this.schema = loadSchema("/avro/processed-event.avsc");
        this.datumWriter = new GenericDatumWriter<>(schema);
    }

    public byte[] write(OutputEvent event) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("recordId", event.recordId());
        record.put("eventType", new GenericData.EnumSymbol(schema.getField("eventType").schema(), event.eventType().name()));
        record.put("name", event.name());
        record.put("processingStatus", event.processingStatus());
        record.put("processedAt", event.processedAt());

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to serialize output Avro event: " + ex.getMessage(), ex);
        }
    }

    private static Schema loadSchema(String path) {
        try (InputStream inputStream = AvroEventWriter.class.getResourceAsStream(path)) {
            return new Schema.Parser().parse(Objects.requireNonNull(inputStream, "Missing schema " + path));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to load Avro schema " + path, ex);
        }
    }
}
