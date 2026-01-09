package com.example.kafkaredis.domain.paymentHistory.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.kafkaredis.common.entity.PaymentHistory;
import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.paymentHistory.repository.PaymentHistoryRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentHistoryService {

	private final PaymentHistoryRepository paymentHistoryRepository;

	//리스너에서 읽어온 값을 DB 에 저장하는 메서드
	@Transactional
	public void savePaymentHistory(PaymentCompletedEvent event) {

		PaymentHistory paymentHistory = PaymentHistory.from(event); //정적 팩토리 메서드를 이용해서 변한

		// 이미 처리된 paymentId라면 재처리하지 않습니다.
		if (paymentHistoryRepository.existsByPaymentId(event.getPaymentId())) {
			log.info("[DB] 이미 처리된 결제입니다. (paymentId={}) - 재처리 스킵", event.getPaymentId());
			return;
		}
		paymentHistoryRepository.save(paymentHistory);
		log.info("[DB] : 결제 기록 저장 완료! paymentId : {}, productId : {}", paymentHistory.getPaymentId(), paymentHistory.getProductId());
	}
}
