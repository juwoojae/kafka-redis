package com.example.kafkaredis.common.config.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;

@Configuration
public class KafkaConsumerConfig {

	// 카프카 주소
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	private Map<String, Object> baseConsumerProps(String groupId) {
		Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		return props;
	}

	private ConsumerFactory<String, PaymentCompletedEvent> buildConsumerFactory(String groupId) {
		JsonDeserializer<PaymentCompletedEvent> deserializer = new JsonDeserializer<>(PaymentCompletedEvent.class);

		return new DefaultKafkaConsumerFactory<>(
			baseConsumerProps(groupId),
			new StringDeserializer(),
			deserializer
		);
	}

	/**
	 * 1. product-ranking-group 컨슈머 그룹을 처리하는 컨슈머
	 *  ConsumerFactory  <String, PaymentCompletedEvent>
	 *  컨슘 한 정보를 기준으로 오늘 가장 많이 판매된 상품의 랭킹을 구하는 작업을 수행한다.
	 */
	@Bean
	public ConsumerFactory<String, PaymentCompletedEvent> productRankingConsumerFactory() {

		return buildConsumerFactory("product-ranking-group");
	}

	// ListenerContainer 카프카 토픽에 값이 들어왔는지 안들어왔는지 감지
	// Listener 에 ConsumerFactory 등록하기
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> paymentListenerContainerFactory(
		CommonErrorHandler commonErrorHandlerWithDLT
	) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent>();
		factory.setConsumerFactory(paymentHistoryConsumerFactory());
		factory.setCommonErrorHandler(commonErrorHandlerWithDLT);
		return factory;
	}

	/**
	 * 2. payment-history-group 컨슈머 그룹을 처리하는 컨슈머
	 * 결제 기록 전용 컨슈머 그룹 만들어주기
	 * @return
	 */
	@Bean
	public ConsumerFactory<String, PaymentCompletedEvent> paymentHistoryConsumerFactory() {

		return buildConsumerFactory("payment-history-group");
	}

	// ListenerContainer 카프카 토픽에 값이 들어왔는지 안들어왔는지 감지
	// Listener 에 ConsumerFactory 등록하기
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> paymentHistoryKafkaListenerContainerFactory(
		CommonErrorHandler commonErrorHandlerWithDLT
	) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent>();
		factory.setConsumerFactory(paymentHistoryConsumerFactory());
		factory.setCommonErrorHandler(commonErrorHandlerWithDLT);
		return factory;
	}

	/**
	 * 3. delivery-group 컨슈머 그룹을 처리하는 컨슈머
	 * 배송처리를 하는 consumerFactory
	 */

	@Bean
	public ConsumerFactory<String, PaymentCompletedEvent> deliveryConsumerFactory() {

		return buildConsumerFactory("delivery-group");
	}

	// ListenerContainer 카프카 토픽에 값이 들어왔는지 안들어왔는지 감지
	// Listener 에 ConsumerFactory 등록하기
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> deliveryKafkaListenerContainerFactory(
		CommonErrorHandler commonErrorHandlerWithDLT
	) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent>();
		factory.setConsumerFactory(deliveryConsumerFactory());
		factory.setCommonErrorHandler(commonErrorHandlerWithDLT);
		return factory;
	}

	// =====================================================================================
	// 공통 DLT ErrorHandler
	// =====================================================================================
	@Bean
	public CommonErrorHandler commonErrorHandlerWithDLT(
		KafkaTemplate<String, PaymentCompletedEvent> paymentCompletedKafkaTemplate) {

		//재처리 로직에도 불고하고 실패한경우 DLT 로 보내기
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(paymentCompletedKafkaTemplate);

		// → 1초 간격으로 2회 재시도 (총 3회)
		FixedBackOff backOff = new FixedBackOff(1000L, 2L);

		return new DefaultErrorHandler(recoverer, backOff);
	}
}
