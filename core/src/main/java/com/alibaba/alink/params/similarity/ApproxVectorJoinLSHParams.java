package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.shared.clustering.HasApproxDistanceType;
import com.alibaba.alink.params.shared.clustering.HasDistanceThreshold;

/**
 * Params for ApproxVectorJoinLSH.
 */
public interface ApproxVectorJoinLSHParams<T> extends
	BaseJoinTopNLSHParams<T>,
	HasApproxDistanceType<T>,
	HasDistanceThreshold<T>{
}
