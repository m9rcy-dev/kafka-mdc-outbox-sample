package com.example.kafkaoutbox.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class KafkaHeaderJsonCodec {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<StoredHeader>> HEADER_LIST = new TypeReference<>() {};

    private KafkaHeaderJsonCodec() {
    }

    public static String toJson(Headers headers) {
        try {
            List<StoredHeader> stored = new ArrayList<>();
            headers.forEach(header -> stored.add(new StoredHeader(
                    header.key(),
                    Base64.getEncoder().encodeToString(header.value())
            )));
            return OBJECT_MAPPER.writeValueAsString(stored);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize Kafka headers", ex);
        }
    }

    public static void applyTo(Headers target, String json) {
        if (json == null || json.isBlank()) {
            return;
        }
        try {
            List<StoredHeader> stored = OBJECT_MAPPER.readValue(json, HEADER_LIST);
            for (StoredHeader header : stored) {
                target.add(header.key(), Base64.getDecoder().decode(header.base64Value()));
            }
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize Kafka headers", ex);
        }
    }

    private record StoredHeader(String key, String base64Value) {
    }
}
