package com.example.kafkaredis.domain.delivery.listener;

import static com.example.kafkaredis.common.model.kafka.topic.KafkaTopic.*;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.delivery.service.DeliveryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeliveryListener {

	private final DeliveryService deliveryService;
	// 결제 완료 토픽 구독 -> 데이터 받아오기
	// 받아온 데이터는 배송 전용 기능으로서 사용한다

	/**
	 * 결제 완료 이벤트가 발생하면
	 * 배송을 바로 시작한다
	 * 배송 준비 -> 배송중 -> 배송 완료
	 * 결제 완료 이벤트를 컨슘하면 배송준비 상태인 배송 데이터 만들기
	 * 만든 배송 데이터를 DB 에 저장하기 (Delivery Entity 로)
	 */
	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "delivery-group",
		containerFactory = "deliveryKafkaListenerContainerFactory"
	)
	public void consume1(PaymentCompletedEvent event) {


		// 테스트용 : 특정 productId에 대해서는 일부러 예외 발생
		if (event.getProductId() == 999L) {
			log.info("[Delivery-Consumer] 테스트용 예외 발생 - productId=999");
			throw new RuntimeException("테스트용 배송 에러");
		}
		deliveryService.createDeliveryFromPayment(event);

		log.info("[Delivery-Consumer] 결제완료 이벤트 수신성공 : orderId : {}, userId : {}"
			, event.getOrderId(), event.getUserId());
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "delivery-group",
		containerFactory = "deliveryKafkaListenerContainerFactory"
	)
	public void consume2(PaymentCompletedEvent event) {


		// 테스트용 : 특정 productId에 대해서는 일부러 예외 발생
		if (event.getProductId() == 999L) {
			log.info("[Delivery-Consumer] 테스트용 예외 발생 - productId=999");
			throw new RuntimeException("테스트용 배송 에러");
		}

		deliveryService.createDeliveryFromPayment(event);
		log.info("[Delivery-Consumer] 결제완료 이벤트 수신성공 : orderId : {}, userId : {}"
			, event.getOrderId(), event.getUserId());
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "delivery-group",
		containerFactory = "deliveryKafkaListenerContainerFactory"
	)
	public void consume3(PaymentCompletedEvent event) {


		// 테스트용 : 특정 productId에 대해서는 일부러 예외 발
		if (event.getProductId() == 999L) {
			log.info("[Delivery-Consumer] 테스트용 예외 발생 - productId=999");
			throw new RuntimeException("테스트용 배송 에러");
		}

		deliveryService.createDeliveryFromPayment(event);
		log.info("[Delivery-Consumer] 결제완료 이벤트 수신성공 : orderId : {}, userId : {}"
			, event.getOrderId(), event.getUserId());
	}


}
