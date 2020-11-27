package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params for QuantileDiscretizerTrain.
 */
public interface QuantileDiscretizerTrainParams<T> extends
	HasSelectedCols <T>,
	HasNumBuckets <T>,
	HasNumBucketsArray <T>,
	HasLeftOpen <T> {
}
