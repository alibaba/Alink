package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * parameters of one hot train process.
 */
public interface OneHotTrainParams<T> extends WithParams <T>,
	HasSelectedCols <T>,
	HasDiscreteThresholds <T>,
	HasDiscreteThresholdsArray <T> {
}
