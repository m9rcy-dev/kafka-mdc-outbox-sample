package com.example.kafkaoutbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaMdcOutboxSampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaMdcOutboxSampleApplication.class, args);
    }
}
