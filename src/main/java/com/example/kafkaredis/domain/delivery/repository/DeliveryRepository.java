package com.example.kafkaredis.domain.delivery.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.kafkaredis.common.entity.Delivery;
import com.example.kafkaredis.common.enums.DeliveryStatus;

public interface DeliveryRepository extends JpaRepository<Delivery,Long> {

	// 배송 상태를 기준으로 최신 20 개 데이터만 가지고 오기
	List<Delivery> findTop20ByUserIdOrderByStatusUpdateAtDesc(Long userId);

	// 스케줄러에서 상태 변경 대상 조회할 때 사용
	List<Delivery> findByDeliveryStatusAndStatusUpdateAtBefore(
		DeliveryStatus deliveryStatus
		, LocalDateTime statusUpdateAtBefore);
}
