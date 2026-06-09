package com.example.kafkaoutbox.common;

public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
