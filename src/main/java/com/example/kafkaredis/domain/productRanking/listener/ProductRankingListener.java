package com.example.kafkaredis.domain.productRanking.listener;

import static com.example.kafkaredis.common.model.kafka.topic.KafkaTopic.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import com.example.kafkaredis.domain.productRanking.service.ProductRankingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProductRankingListener {

	private final ProductRankingService productRankingService;

	// 아까 등록한 ConsumerFactory, ListenerContainerFactory 를 통해서
	// 카프카에 있는 데이터를 가져온다.
	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "product-ranking-group",
		containerFactory = "paymentListenerContainerFactory"
	)
	public void consumer1(PaymentCompletedEvent event) {

		log.info("[상품 랭킹 조회 리스너] : 성공적으로 값을 잘 가지고 옴");

		// 결제 완료된 시간을 기준으로 판매 완료 랭킹을 반영한다

		// 결제 완료된 시간을 String -> LocalDateTime 으로 먼저 받고
		// LocalDateTime -> LocalDate 로 받아서 처리하기
		LocalDateTime paidAt = LocalDateTime.parse(event.getPaidAt());
		LocalDate currentDate = paidAt.toLocalDate();

		productRankingService.increaseProductRanking(event.getProductId(), currentDate);
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "product-ranking-group",
		containerFactory = "paymentListenerContainerFactory"
	)
	public void consumer2(PaymentCompletedEvent event) {

		log.info("[상품 랭킹 조회 리스너] : 성공적으로 값을 잘 가지고 옴");

		// 결제 완료된 시간을 기준으로 판매 완료 랭킹을 반영한다

		// 결제 완료된 시간을 String -> LocalDateTime 으로 먼저 받고
		// LocalDateTime -> LocalDate 로 받아서 처리하기
		LocalDateTime paidAt = LocalDateTime.parse(event.getPaidAt());
		LocalDate currentDate = paidAt.toLocalDate();

		productRankingService.increaseProductRanking(event.getProductId(), currentDate);
	}

	@KafkaListener(
		topics = TOPIC_PAYMENT_COMPLETED,
		groupId = "product-ranking-group",
		containerFactory = "paymentListenerContainerFactory"
	)
	public void consumer3(PaymentCompletedEvent event) {

		log.info("[상품 랭킹 조회 리스너] : 성공적으로 값을 잘 가지고 옴");

		// 결제 완료된 시간을 기준으로 판매 완료 랭킹을 반영한다

		// 결제 완료된 시간을 String -> LocalDateTime 으로 먼저 받고
		// LocalDateTime -> LocalDate 로 받아서 처리하기
		LocalDateTime paidAt = LocalDateTime.parse(event.getPaidAt());
		LocalDate currentDate = paidAt.toLocalDate();

		productRankingService.increaseProductRanking(event.getProductId(), currentDate);
	}
}
