package com.example.kafkaredis.common.model.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RankingDto {

	String title;
	private double score;
}
