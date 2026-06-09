package com.example.kafkaoutbox.domain;

public record InputEvent(
        String recordId,
        EventType eventType,
        String name
) {
}
