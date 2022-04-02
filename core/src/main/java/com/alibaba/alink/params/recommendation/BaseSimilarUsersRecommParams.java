package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;

/**
 * parameters of SimilarUsers Recommender.
 */
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "initRecommCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
public interface BaseSimilarUsersRecommParams<T> extends
	HasUserCol <T>,
	HasKDefaultAs10 <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
