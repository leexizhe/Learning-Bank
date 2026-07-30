package com.kafkabank.payment.repository;

import com.kafkabank.payment.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {}
