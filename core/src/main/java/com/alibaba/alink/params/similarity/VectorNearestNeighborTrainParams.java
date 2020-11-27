package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.shared.clustering.HasFastMetric;

/**
 * Params for ApproxStringNearestNeighborIndex.
 */
public interface VectorNearestNeighborTrainParams<T> extends
	NearestNeighborTrainParams <T>,
	HasFastMetric <T> {
}
