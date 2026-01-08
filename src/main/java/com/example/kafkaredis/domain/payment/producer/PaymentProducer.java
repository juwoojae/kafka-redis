package com.example.kafkaredis.domain.payment.producer;

import static com.example.kafkaredis.common.model.kafka.topic.KafkaTopic.*;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.kafkaredis.common.model.kafka.event.PaymentCompletedEvent;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymentProducer {

	// 카프카에 메시지를 보내주는 역할 수행
	// 스프링 부트와 카프카가 소통할때 필요하느 kafkaTemplate 선언
	private final KafkaTemplate<String, PaymentCompletedEvent> paymentCompletedEventProducerFactory;

	public void send(PaymentCompletedEvent event){

		paymentCompletedEventProducerFactory.send(TOPIC_PAYMENT_COMPLETED, event);
	}
}
