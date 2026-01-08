package com.example.kafkaredis.domain.delivery.service;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.entity.Delivery;
import com.example.kafkaredis.common.enums.DeliveryStatus;
import com.example.kafkaredis.domain.delivery.modell.response.DeliveryResponse;
import com.example.kafkaredis.domain.delivery.repository.DeliveryRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryCacheService {

	private final DeliveryRepository deliveryRepository;
	private final StringRedisTemplate stringRedisTemplate;

	// 배송 상태를 DB 뿐만 아니라 redis 캐시에도 저장한다.
	public void cacheDelivery(Delivery delivery) {

		// Redis 에 저장할 키 만들기
		// deliveryId 기준으로 어떤 상태값을 가지고 있는지 redis 캐시에 저장 하기 위한 목직
		String key = "delivery:status:" + delivery.getId();

		// redis 에 값 넣어주기 (hash 자료구조 = 객체)
		stringRedisTemplate.opsForHash().putAll(key, Map.of(
			"status", delivery.getDeliveryStatus().name(), //배송 상태
			"orderId", delivery.getOrderId().toString(), // 주문 아이디
			"productId", delivery.getProductId().toString(), // 상품 아이디
			"statusUpdateAt", delivery.getStatusUpdateAt().toString() // 배송 상태가 언제 업데이트 되었는지.
		));

		// 사용자별 배송 목록 조회 기능 만들어주기
		// userId 를 기준으로 현재 내가 주문한 상품의 배송 상태를 조회하는 기능
		// 정렬을 배송 상태 업데이트 기준으로 정렬 (배송 완료 > 배송 중 > 배송 준비)

		// zset , sorted set
		// key , value , 점수
		// key : user_deliveries:{userId}
		// key 세팅
		String userDeliveryKey = "user_deliveries:" + delivery.getUserId();

		// value 셋팅 delivery.getId();

		// score 세팅 (localDateTime 을 점수화 해야한다) epoch 사용 (시간을 숫자로 변환)
		double score = delivery.getStatusUpdateAt().toEpochSecond(ZoneOffset.UTC);

		// redis 에 값을 넣어주면 끝
		stringRedisTemplate.opsForZSet()
			.add(userDeliveryKey, delivery.getId().toString(), score);

		log.info("[Delivery-Redis] 캐시에 저장 완료! key : {} , status : {}", key, delivery.getDeliveryStatus());
	}

	// DB 가 아니라 캐시 데이터를 기준으로 조회하는 방법
	// 유저별 최신 배송 정보 조회 최근순 20 개
	public List<DeliveryResponse> findUserDeliveries(long userId) {

		// 값을 조회

		// key 로 어떤 redis 데이터 조회할 것인지

		// 유저 아이디 별로 배송 상태 최신화 목록 조회를 만들것임.

		// user:deliveries:userId  배송 상태 최신화 목록 20개

		String userKey = "user_deliveries:" + userId;

		Set<String> deliveryIdList =
			stringRedisTemplate.opsForZSet().reverseRangeByScore(userKey, 0, 19);

		// 캐시에 ID 리스트가 없다면, DB 에서 조회하면 된다
		if (deliveryIdList == null || deliveryIdList.isEmpty()) {
			return deliveryRepository.findTop20ByUserIdOrderByStatusUpdateAtDesc(userId)
				.stream()
				.map(delivery -> new DeliveryResponse(
					delivery.getId(),
					delivery.getOrderId(),
					delivery.getProductId(),
					delivery.getDeliveryStatus(),
					delivery.getStatusUpdateAt().toString()
				))
				.toList();
		}

		// 최신화 목록 캐시가 있다면, 그럼 DB 에 직접 조회 할 필요 없이 캐시에서 필요한 데이터를 가지고 오면 된다.

		List<DeliveryResponse> result = new ArrayList<>();

		for(String deliveryId : deliveryIdList) {

			String key = "delivery:status:" + deliveryId;

			Map<Object, Object> cached = stringRedisTemplate.opsForHash().entries(key);

			// 캐시가 없다면
			if(cached.isEmpty()) {
				// DB 에 직접 조회해서 가지고 오면 된다.
				Delivery delivery = deliveryRepository.findById(Long.valueOf(deliveryId)).orElse(null);
				result.add(new DeliveryResponse(
					delivery.getId(),
					delivery.getOrderId(),
					delivery.getProductId(),
					delivery.getDeliveryStatus(),
					delivery.getStatusUpdateAt().toString()
				));
			}
			// 만약 캐시가 존재한다면 (redis 에 있는것은 string 이라서, 전부 형변환을 해줘야해)
			DeliveryResponse response = new DeliveryResponse(
				Long.valueOf(deliveryId),
				Long.valueOf(cached.get("orderId").toString()),
				Long.valueOf(cached.get("productId").toString()),
				DeliveryStatus.valueOf(cached.get("status").toString()),
				cached.get("statusUpdatedAt").toString()
			);

			result.add(response);
		}
		return result;
	}
}
