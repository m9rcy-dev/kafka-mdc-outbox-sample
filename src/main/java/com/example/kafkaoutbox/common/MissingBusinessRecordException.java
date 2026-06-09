package com.example.kafkaoutbox.common;

public class MissingBusinessRecordException extends RuntimeException {
    public MissingBusinessRecordException(String recordId) {
        super("Business record does not exist yet for recordId=" + recordId);
    }
}
