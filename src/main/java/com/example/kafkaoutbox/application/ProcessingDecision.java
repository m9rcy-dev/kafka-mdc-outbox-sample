package com.example.kafkaoutbox.application;

public record ProcessingDecision(
        PublishMode publishMode,
        byte[] plainOutputPayload
) {

    public static ProcessingDecision outboxed() {
        return new ProcessingDecision(PublishMode.OUTBOXED, null);
    }

    public static ProcessingDecision directPublish(byte[] plainOutputPayload) {
        return new ProcessingDecision(PublishMode.DIRECT_PUBLISH, plainOutputPayload);
    }
}
