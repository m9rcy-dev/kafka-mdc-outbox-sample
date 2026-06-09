package com.example.kafkaoutbox.outbox;

public enum OutboxStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
