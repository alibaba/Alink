package com.alibaba.alink.params.recommendation;

/**
 * parameters of Rate Recommender.
 */
public interface BaseRateRecommParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	BaseRecommParams <T> {
}
