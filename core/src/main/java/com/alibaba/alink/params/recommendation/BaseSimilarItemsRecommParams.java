package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;

/**
 * parameters of SimilarItems Recommender.
 */
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "initRecommCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
public interface BaseSimilarItemsRecommParams<T> extends
	HasItemCol <T>,
	HasKDefaultAs10 <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
