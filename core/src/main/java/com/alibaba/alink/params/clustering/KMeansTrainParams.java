package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * Params for KMeansTrainer.
 */
public interface KMeansTrainParams<T> extends
	HasKMeansDistanceType<T>,
	HasVectorCol <T>,
	BaseKMeansTrainParams <T> {
}
