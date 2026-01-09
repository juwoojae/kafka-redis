package com.example.kafkaredis.domain.paymentHistory.listener;

import static com.example.kafkaredis.common.model.kafka.topic.KafkaTopic.*;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.paymentHistory.service.PaymentHistoryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentHistoryListener {

	private final PaymentHistoryService paymentHistoryService;

	// payment-completed 토픽을 구독하는 토픽의 데이터를 읽어오는 리스너를 만들자
	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "payment-history-group",
		containerFactory = "paymentHistoryKafkaListenerContainerFactory"
	)
	public void consume1(PaymentCompletedEvent event) {

		log.info("[Consumer-History] 결제 완료 이벤트 수신! paymentId : {}, productId :{}"
			, event.getPaymentId(), event.getProductId());

		// DB 에 결제 왐료 기록을 저장
		paymentHistoryService.savePaymentHistory(event);
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "payment-history-group",
		containerFactory = "paymentHistoryKafkaListenerContainerFactory"
	)
	public void consume2(PaymentCompletedEvent event) {

		log.info("[Consumer-History] 결제 완료 이벤트 수신! paymentId : {}, productId :{}"
			, event.getPaymentId(), event.getProductId());

		// DB 에 결제 왐료 기록을 저장
		paymentHistoryService.savePaymentHistory(event);
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "payment-history-group",
		containerFactory = "paymentHistoryKafkaListenerContainerFactory"
	)
	public void consume3(PaymentCompletedEvent event) {

		log.info("[Consumer-History] 결제 완료 이벤트 수신! paymentId : {}, productId :{}"
			, event.getPaymentId(), event.getProductId());

		// DB 에 결제 왐료 기록을 저장
		paymentHistoryService.savePaymentHistory(event);
	}
}
