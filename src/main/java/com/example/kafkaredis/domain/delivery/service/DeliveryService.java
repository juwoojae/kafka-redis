package com.example.kafkaredis.domain.delivery.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.entity.Delivery;
import com.example.kafkaredis.common.enums.DeliveryStatus;
import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.delivery.repository.DeliveryRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class DeliveryService {

	private final DeliveryRepository deliveryRepository;
	private final DeliveryCacheService deliveryCacheService;

	// 리스너에서 받아온 이벤트를 기준으로 배송 데이터 생성해주기
	public void createDeliveryFromPayment(PaymentCompletedEvent event) {

		// 해당 이벤트를 기준으로 배송 데이터 생성

		//결제 완료된 시간을 String -> localDateTime 으로 변환
		LocalDateTime paidAt = LocalDateTime.parse(
			event.getPaidAt(),
			DateTimeFormatter.ISO_LOCAL_DATE_TIME
		);

		Delivery delivery = Delivery.builder()
			.orderId(event.getOrderId())
			.paymentId(event.getPaymentId())
			.productId(event.getProductId())
			.userId(event.getUserId())
			.deliveryStatus(DeliveryStatus.PREPARED)
			.paidAt(paidAt)
			.statusUpdateAt(paidAt)
			.build();

		deliveryRepository.save(delivery);

		log.info("[Delivery DB] 배송 준비 생성 - orderId : {}, status : {} "
		, delivery.getOrderId(), delivery.getDeliveryStatus());

		deliveryCacheService.cacheDelivery(delivery);// DB 에 저장함과 동시에 redis 캐시에도 저장한다
	}
}
