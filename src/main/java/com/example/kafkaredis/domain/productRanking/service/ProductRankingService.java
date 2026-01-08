package com.example.kafkaredis.domain.productRanking.service;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.*;
import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.model.redis.RankingDto;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ProductRankingService {
	// ProductRankingListener에서 받은 메시지를 기반으로

	// 오늘 가장 많이 판매된 상품 리스트 랭킹만들어 주기

	// redis 의 sorted set 사용하기
	// StringRedisTemplate 를 통해서 redis 에 랭킹 데이터를 만들기

	private final StringRedisTemplate stringRedisTemplate;

	public static final String PRODUCT_RANKING_DAILY_KEY = "product:ranking:";

	// 받아온 결제 완료 정보를 기준으로 오늘 판매된 상품 랭킹을 만들어주는 메서드
	public void increaseProductRanking(long productId, LocalDate currentDate) {

		// 오늘 판매 완료된 상품을 저장할 키
		// 2025-12-25 -> product:ranking:2025-12-25
		String key = PRODUCT_RANKING_DAILY_KEY + currentDate.toString();

		stringRedisTemplate.opsForZSet().incrementScore(key, String.valueOf(productId), 1);
	}

	// 저장된 redis의 오늘 가장 많이 팔린 상품 TOP3 조회하기

	public List<RankingDto> findProductRankingTop3InToday() {

		LocalDate currentDate = LocalDate.now();

		String key = PRODUCT_RANKING_DAILY_KEY + currentDate.toString();

		Set<TypedTuple<String>> result = stringRedisTemplate.opsForZSet()
			.reverseRangeWithScores(key, 0, 2); // TOP 3

		if(result.isEmpty()) {
			return Collections.emptyList();
		}

		return result.stream()
			.map(tuple -> new RankingDto(tuple.getValue(), tuple.getScore()))
			.toList();
	}
}
