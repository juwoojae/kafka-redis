package com.example.kafkaredis.domain.productRanking.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaredis.common.model.redis.RankingDto;
import com.example.kafkaredis.domain.productRanking.service.ProductRankingService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/ranking/product")
public class ProductRankingController
{
	private final ProductRankingService productRankingService;

	@GetMapping("/today")
	public ResponseEntity<List<RankingDto>> findProductRankingTop3InToday(){

		return ResponseEntity.ok(productRankingService.findProductRankingTop3InToday());
	}
}
