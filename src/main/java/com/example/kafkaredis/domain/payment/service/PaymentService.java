package com.example.kafkaredis.domain.payment.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.payment.model.request.CompletePaymentRequest;
import com.example.kafkaredis.domain.payment.producer.PaymentProducer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymentService {

	private final PaymentProducer producer;

	/**
	 * 결제 완료 이벤트 발생
	 * CompletePaymentRequest 로 들어온것을 Producer 가 PaymentCompletedEvent 로 변환.
	 * @param request
	 */
	public void paymentComplete(CompletePaymentRequest request) {

		// 결제 PG 사 통신 로직
		// 실제 결제 시도 로직 들어가는 가리
		// 이번 실습에서는 실제 결제하는 로직은 제외하고 결제를 성공했다고 가정하고 진행하겠습니다.

		// 결제 시도 로직

		// 결제가 최종적으로 성공했으면 해당 메시지 발행

		// 실제로 결제가 성공한 시간
		// 파싱 에러를 막기 위한 포멧팅
		String paidAt = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

		PaymentCompletedEvent event = PaymentCompletedEvent.builder()
			.paymentId(request.getPaymentId())
			.orderId(request.getOrderId())
			.productId(request.getProductId())
			.userId(request.getUserId())
			.category(request.getCategory())
			.quantity(request.getQuantity())
			.paidAt(paidAt)
			.build();


		producer.send(event);
	}
}
