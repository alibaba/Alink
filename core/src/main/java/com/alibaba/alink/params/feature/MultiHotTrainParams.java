package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.delimiter.HasDelimiterDefaultAsBlank;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params for multi-hot TrainBatchOp.
 */
public interface MultiHotTrainParams<T> extends
	HasSelectedCols <T>,
	HasDelimiterDefaultAsBlank <T>,
	HasDiscreteThresholds <T>,
	HasDiscreteThresholdsArray <T> {
}
