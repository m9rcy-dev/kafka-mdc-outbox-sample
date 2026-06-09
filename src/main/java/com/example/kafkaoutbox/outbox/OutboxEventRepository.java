package com.example.kafkaoutbox.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    boolean existsByRecordIdAndStatusIn(String recordId, Collection<OutboxStatus> statuses);

    @Query(value = """
            SELECT *
            FROM outbox_event
            WHERE status = 'PENDING'
            ORDER BY source_topic, source_partition, source_offset
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
            """, nativeQuery = true)
    List<OutboxEvent> findPendingForPublish(int limit);
}
