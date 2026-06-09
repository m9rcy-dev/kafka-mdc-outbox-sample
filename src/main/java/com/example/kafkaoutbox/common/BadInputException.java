package com.example.kafkaoutbox.common;

public class BadInputException extends RuntimeException {
    public BadInputException(String message) {
        super(message);
    }
}
