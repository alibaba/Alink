package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.shared.HasNumThreads;

public interface VectorApproxNearestNeighborLocalTrainParams<T> extends
	VectorApproxNearestNeighborTrainParams <T>, HasMaxNumCandidates <T>, HasNumThreads <T> {
}
