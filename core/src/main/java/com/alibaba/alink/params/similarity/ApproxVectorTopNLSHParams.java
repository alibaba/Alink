package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.shared.clustering.HasApproxDistanceType;

/**
 * Params for ApproxVectorTopNLSH.
 */
public interface ApproxVectorTopNLSHParams<T> extends
	BaseJoinTopNLSHParams<T>,
	HasApproxDistanceType<T>,
	HasTopN_5<T>{
}
