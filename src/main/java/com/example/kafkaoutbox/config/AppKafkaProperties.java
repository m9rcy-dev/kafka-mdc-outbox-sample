package com.example.kafkaoutbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record AppKafkaProperties(
        String inputTopic,
        String processedTopic,
        String dlqSuffix
) {
}
