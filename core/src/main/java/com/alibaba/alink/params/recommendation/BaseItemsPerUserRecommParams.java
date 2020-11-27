package com.alibaba.alink.params.recommendation;

/**
 * parameters of ItemsPerUser Recommender.
 */
public interface BaseItemsPerUserRecommParams<T> extends
	HasUserCol <T>,
	HasKDefaultAs10 <T>,
	HasExcludeKnownDefaultAsFalse <T>,
	BaseRecommParams <T> {
}
