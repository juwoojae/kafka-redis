package com.example.kafkaredis.common.config.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;

@EnableKafka
@Configuration
public class KafkaProduceConfig {

	// 스프링 부트에 카프카 클러스터 주소를 연결
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	// ProduceFactory 만들어주기
	// 스프링 부트에서 발생한 이벤트를 Kafka 에 저장시키는 역할을 수행한다
	@Bean
	public ProducerFactory<String, PaymentCompletedEvent> eventProducerFactory() {

		Map<String, Object> props = new HashMap<>();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(props);
	}

	// KafkaTemplate 을 하나 만들어줄 예정
	// 스프링 부트와 카프카가 소통할 때 사용하는 객체
	@Bean
	public KafkaTemplate<String, PaymentCompletedEvent> paymentCompletedEventProducerFactory() {
		return new KafkaTemplate<>(eventProducerFactory());
	}

}
