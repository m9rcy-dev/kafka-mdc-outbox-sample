package com.example.kafkaoutbox.record;

import com.example.kafkaoutbox.domain.InputEvent;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "business_record")
public class BusinessRecord {

    @Id
    private String recordId;
    private String name;
    private String sourceEventType;
    private Instant createdAt;
    private Instant updatedAt;

    protected BusinessRecord() {
    }

    private BusinessRecord(String recordId, String name, String sourceEventType, Instant now) {
        this.recordId = recordId;
        this.name = name;
        this.sourceEventType = sourceEventType;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static BusinessRecord fromNewEvent(InputEvent event) {
        return new BusinessRecord(event.recordId(), event.name(), event.eventType().name(), Instant.now());
    }
}
