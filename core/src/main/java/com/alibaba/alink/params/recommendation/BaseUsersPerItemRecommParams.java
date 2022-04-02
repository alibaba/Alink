package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;

/**
 * parameters of UsersPerItem Recommender.
 */
@ParamSelectColumnSpec(name = "itemCol")
@ParamSelectColumnSpec(name = "initRecommCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
public interface BaseUsersPerItemRecommParams<T> extends
	HasItemCol <T>,
	HasKDefaultAs10 <T>,
	HasExcludeKnownDefaultAsFalse <T>,
	BaseRecommParams <T>,
	HasInitRecommCol<T> {
}
