package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasDistanceType;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * Params for KMeansTrainer.
 */
public interface KMeansTrainParams<T> extends
	HasDistanceType <T>,
	HasVectorCol <T>,
	BaseKMeansTrainParams <T> {
}
