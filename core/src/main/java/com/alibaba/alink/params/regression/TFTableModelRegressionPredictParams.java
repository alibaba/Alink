package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * parameters of TF Table model regressor prediction.
 */
public interface TFTableModelRegressionPredictParams<T> extends
	HasPredictionCol <T>, HasReservedColsDefaultAsNull <T>, HasInferBatchSizeDefaultAs256 <T> {
}
