package com.example.kafkaredis.domain.paymentHistory.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.kafkaredis.common.entity.PaymentHistory;

public interface PaymentHistoryRepository extends JpaRepository<PaymentHistory, Long> {
}
