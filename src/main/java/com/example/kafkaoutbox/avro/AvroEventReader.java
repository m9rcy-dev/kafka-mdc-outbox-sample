package com.example.kafkaoutbox.avro;

import com.example.kafkaoutbox.common.BadInputException;
import com.example.kafkaoutbox.domain.EventType;
import com.example.kafkaoutbox.domain.InputEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Reads decrypted Avro binary payloads.
 * The listener still consumes byte[] because encryption is handled before Avro deserialization.
 */
@Component
public class AvroEventReader {

    private final Schema schema;
    private final DatumReader<GenericRecord> datumReader;

    public AvroEventReader() {
        this.schema = loadSchema("/avro/input-event.avsc");
        this.datumReader = new GenericDatumReader<>(schema);
    }

    public InputEvent read(byte[] bytes, Headers headers) {
        try {
            GenericRecord record = datumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
            return new InputEvent(
                    requiredString(record, "recordId"),
                    EventType.valueOf(requiredString(record, "eventType")),
                    requiredString(record, "name")
            );
        } catch (IOException | IllegalArgumentException ex) {
            throw new BadInputException("Unable to deserialize input Avro event: " + ex.getMessage());
        }
    }

    private static Schema loadSchema(String path) {
        try (InputStream inputStream = AvroEventReader.class.getResourceAsStream(path)) {
            return new Schema.Parser().parse(Objects.requireNonNull(inputStream, "Missing schema " + path));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to load Avro schema " + path, ex);
        }
    }

    private String requiredString(GenericRecord record, String field) {
        Object value = record.get(field);
        if (value == null || value.toString().isBlank()) {
            throw new BadInputException("Missing required field: " + field);
        }
        return value.toString();
    }
}
