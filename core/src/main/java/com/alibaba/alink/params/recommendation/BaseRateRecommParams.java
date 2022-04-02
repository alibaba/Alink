package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;

/**
 * parameters of Rate Recommender.
 */
@ParamSelectColumnSpec(name = "userCol")
@ParamSelectColumnSpec(name = "itemCol")
public interface BaseRateRecommParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	BaseRecommParams <T> {
}
