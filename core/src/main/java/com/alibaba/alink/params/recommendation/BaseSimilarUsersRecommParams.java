package com.alibaba.alink.params.recommendation;

/**
 * parameters of SimilarUsers Recommender.
 */
public interface BaseSimilarUsersRecommParams<T> extends
	HasUserCol <T>,
	HasKDefaultAs10 <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
