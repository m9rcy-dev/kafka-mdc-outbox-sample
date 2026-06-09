package com.example.kafkaoutbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.outbox")
public record AppOutboxProperties(
        boolean enabled,
        long fixedDelayMs,
        int batchSize
) {
}
