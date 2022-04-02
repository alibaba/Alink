package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;

/**
 * parameters of ItemsPerUser Recommender.
 */
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "initRecommCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
public interface BaseItemsPerUserRecommParams<T> extends
	HasUserCol <T>,
	HasKDefaultAs10 <T>,
	HasExcludeKnownDefaultAsFalse <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
