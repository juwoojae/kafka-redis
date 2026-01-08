package com.example.kafkaredis.scheduler;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.entity.Delivery;
import com.example.kafkaredis.common.enums.DeliveryStatus;
import com.example.kafkaredis.domain.delivery.repository.DeliveryRepository;
import com.example.kafkaredis.domain.delivery.service.DeliveryCacheService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryStatusScheduler {

	private final DeliveryRepository deliveryRepository;
	private final DeliveryCacheService deliveryCacheService;

	@Scheduled(fixedRate = 15000) // 15초 에 한번씩 해당 메서드를 실행한다.
	@Transactional
	// 배송 준비중이였던 데이터를 배송 중으로 변경
	public void updatePreparingToShipping() {

		// 10 초 지난 값들만 바꿔줄 예정
		LocalDateTime now = LocalDateTime.now();
		LocalDateTime threshold =now.minusSeconds(10); //현재 시간 기준으로 10초 전 LocalDateTime

		List<Delivery> preparingList =
			deliveryRepository.findByDeliveryStatusAndStatusUpdateAtBefore(
				DeliveryStatus.PREPARED,
				threshold
			);

		if (preparingList.isEmpty()) {
			return ;
		}

		log.info("[Schedular] : PREPARING TO SHIPPING 전환 대상 {}건", preparingList.size());

		for (Delivery delivery : preparingList) {
			delivery.markShipping(now);
			deliveryRepository.save(delivery);
			deliveryCacheService.cacheDelivery(delivery);
		}
	}

	@Scheduled(fixedRate = 15000) // 15초 에 한번씩 해당 메서드를 실행한다.
	@Transactional
	// 배송 준비중이였던 데이터를 배송 중으로 변경
	public void updatePreparingToCompleted() {

		// 10 초 지난 값들만 바꿔줄 예정
		LocalDateTime now = LocalDateTime.now();
		LocalDateTime threshold = now.minusSeconds(10); //현재 시간 기준으로 10초 전 LocalDateTime

		List<Delivery> shippingList =
			deliveryRepository.findByDeliveryStatusAndStatusUpdateAtBefore(
				DeliveryStatus.SHIPPING,
				threshold
			);

		if (shippingList.isEmpty()) {
			return;
		}

		log.info("[Schedular] : PREPARING TO Completed 전환 대상 {}건", shippingList.size());

		for (Delivery delivery : shippingList) {
			delivery.markCompleted(now);
			deliveryRepository.save(delivery);
			deliveryCacheService.cacheDelivery(delivery);
		}

	}
}
