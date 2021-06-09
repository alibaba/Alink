package com.alibaba.alink.params.recommendation;

/**
 * parameters of SimilarItems Recommender.
 */
public interface BaseSimilarItemsRecommParams<T> extends
	HasItemCol <T>,
	HasKDefaultAs10 <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
