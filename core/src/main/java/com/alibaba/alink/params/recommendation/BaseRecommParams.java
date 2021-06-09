package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * common parameters of Recommender.
 */
public interface BaseRecommParams<T> extends
	ModelMapperParams <T>,
	HasRecommCol <T>,
	HasReservedColsDefaultAsNull <T> {
}
