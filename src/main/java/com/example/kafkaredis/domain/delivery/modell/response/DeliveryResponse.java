package com.example.kafkaredis.domain.delivery.modell.response;

import com.example.kafkaredis.common.enums.DeliveryStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DeliveryResponse {

	private Long deliveryId;
	private Long orderId;
	private Long productId;
	private DeliveryStatus deliveryStatus;
	private String statusUpdateAt;  // redis 에 string 으로 들어가니깐 편의성을 위해서.
}
