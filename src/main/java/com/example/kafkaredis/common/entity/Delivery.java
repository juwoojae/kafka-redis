package com.example.kafkaredis.common.entity;

import java.time.LocalDateTime;

import com.example.kafkaredis.common.enums.DeliveryStatus;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "deliveries")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Delivery {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private Long orderId;
	private Long paymentId;
	private Long productId;
	private Long userId;

	@Enumerated(EnumType.STRING)
	private DeliveryStatus deliveryStatus;    // PREPARING / SHIPPING / COMPLETED

	private LocalDateTime paidAt;          // 결제 완료 시각
	private LocalDateTime statusUpdateAt; // 마지막 상태 변경 시각

	public void markShipping(LocalDateTime now) {       // 배송 시작
		this.deliveryStatus = DeliveryStatus.SHIPPING;
		this.statusUpdateAt = now;
	}

	public void markCompleted(LocalDateTime now) {      // 배송 완료
		this.deliveryStatus = DeliveryStatus.COMPLETED;
		this.statusUpdateAt = now;
	}
}