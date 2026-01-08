package com.example.kafkaredis.domain.delivery.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaredis.domain.delivery.modell.response.DeliveryResponse;
import com.example.kafkaredis.domain.delivery.service.DeliveryCacheService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/delivery")
public class DeliveryController {

	private final DeliveryCacheService deliveryCacheService;
	@GetMapping
	public ResponseEntity<List<DeliveryResponse>> findUserDeliveries(@RequestParam Long userId) {

		return ResponseEntity.ok(deliveryCacheService.findUserDeliveries(userId));
	}
}
