package com.example.kafkaoutbox.outbox;

import com.example.kafkaoutbox.config.AppKafkaProperties;
import jakarta.persistence.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_event",
        uniqueConstraints = @UniqueConstraint(name = "uk_outbox_source_position", columnNames = {"sourceTopic", "sourcePartition", "sourceOffset"}),
        indexes = {
                @Index(name = "idx_outbox_record_status", columnList = "recordId,status"),
                @Index(name = "idx_outbox_publish_order", columnList = "status,sourceTopic,sourcePartition,sourceOffset")
        })
public class OutboxEvent {

    @Id
    private UUID id;
    private String recordId;
    private String eventKey;
    private String targetTopic;
    private String sourceTopic;
    private int sourcePartition;
    private long sourceOffset;

    @Lob
    private byte[] payloadBytes;

    @Lob
    private String headersJson;

    private String correlationId;

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    private int retryCount;
    private Instant createdAt;
    private Instant publishedAt;

    @Lob
    private String lastError;

    protected OutboxEvent() {
    }

    public static OutboxEvent from(ConsumerRecord<String, byte[]> record,
                                   String recordId,
                                   byte[] plainOutputPayload,
                                   String correlationId,
                                   AppKafkaProperties properties) {
        OutboxEvent event = new OutboxEvent();
        event.id = UUID.randomUUID();
        event.recordId = recordId;
        event.eventKey = record.key();
        event.targetTopic = properties.processedTopic();
        event.sourceTopic = record.topic();
        event.sourcePartition = record.partition();
        event.sourceOffset = record.offset();
        event.payloadBytes = plainOutputPayload;
        event.headersJson = KafkaHeaderJsonCodec.toJson(record.headers());
        event.correlationId = correlationId;
        event.status = OutboxStatus.PENDING;
        event.retryCount = 0;
        event.createdAt = Instant.now();
        return event;
    }

    public UUID getId() { return id; }
    public String getRecordId() { return recordId; }
    public String getEventKey() { return eventKey; }
    public String getTargetTopic() { return targetTopic; }
    public byte[] getPayloadBytes() { return payloadBytes; }
    public String getHeadersJson() { return headersJson; }
    public String getCorrelationId() { return correlationId; }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
        this.lastError = null;
    }

    public void markFailed(Exception ex) {
        this.status = OutboxStatus.PENDING;
        this.retryCount++;
        this.lastError = ex.getMessage();
    }
}
