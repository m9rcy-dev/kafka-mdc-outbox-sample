package com.example.kafkaoutbox.domain;

public record OutputEvent(
        String recordId,
        EventType eventType,
        String name,
        String processingStatus,
        String processedAt
) {

    public static OutputEvent from(InputEvent inputEvent) {
        return new OutputEvent(
                inputEvent.recordId(),
                inputEvent.eventType(),
                inputEvent.name(),
                "ENRICHED",
                java.time.Instant.now().toString()
        );
    }
}
