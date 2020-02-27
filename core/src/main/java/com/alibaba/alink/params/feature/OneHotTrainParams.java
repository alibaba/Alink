package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * parameters of one hot train process.
 */
public interface OneHotTrainParams<T> extends WithParams<T>,
	HasSelectedCols <T>,
	HasDiscreteThresholds<T>,
	HasDiscreteThresholdsArray<T>{
}
