package com.example.kafkaredis.domain.payment.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.payment.model.request.CompletePaymentRequest;
import com.example.kafkaredis.domain.payment.service.PaymentService;

import lombok.RequiredArgsConstructor;

/**
 * 결제 완료 API 시나리오
 *
 * [1] (가정) 주문은 이미 생성되어 있음
 *     ↓
 * [2] 결제 완료 API 호출 (/api/payment/completion)
 *
 * [3] 결제 완료 API
 *     → PaymentCompletedEvent 생성
 *     → Kafka 토픽 "payment-completed" 에 이벤트 발행
 *
 * [4] Kafka Consumer (product-ranking-group)
 *     → PaymentCompletedEvent 수신
 *     → Redis ZSET에 상품별 판매 수 누적
 *
 * [5] 랭킹 조회 API (/api/ranking/product/today)
 *     → 오늘 가장 많이 판매된 상품 TOP 3 조회
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/payment")
public class PaymentController {

	private final PaymentService paymentService;

	@PostMapping("/completion")
	public ResponseEntity<Void> paymentCompletedEvent(@RequestBody CompletePaymentRequest request){

		paymentService.paymentComplete(request);
		return ResponseEntity.ok().build();
	}
}
