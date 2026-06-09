package com.example.kafkaoutbox.record;

import org.springframework.data.jpa.repository.JpaRepository;

public interface BusinessRecordRepository extends JpaRepository<BusinessRecord, String> {
    boolean existsByRecordId(String recordId);
}
