package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * common parameters of Recommender.
 */
public interface BaseRecommParams<T> extends
	HasRecommCol <T>,
	HasReservedColsDefaultAsNull <T>, HasNumThreads <T> {
}
