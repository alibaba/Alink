package com.alibaba.alink.params.recommendation;

/**
 * parameters of UsersPerItem Recommender.
 */
public interface BaseUsersPerItemRecommParams<T> extends
	HasItemCol <T>,
	HasKDefaultAs10 <T>,
	HasExcludeKnownDefaultAsFalse <T>,
	BaseRecommParams <T> {
}
