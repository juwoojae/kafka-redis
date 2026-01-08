package com.example.kafkaredis.common.config.redis;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

	// 스프링 부트랑 redis 랑 소통할때 필요한 redisTemplate
	// String 타입으로 소통하기
	@Bean
	public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory factory) {

		return new StringRedisTemplate(factory);
	}

}
