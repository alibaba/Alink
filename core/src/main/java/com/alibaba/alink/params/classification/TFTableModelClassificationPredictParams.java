package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * parameters of TF Table model binary classifier prediction.
 */
public interface TFTableModelClassificationPredictParams<T>
	extends HasPredictionCol <T>, HasReservedColsDefaultAsNull <T>, HasPredictionDetailCol <T>,
	HasInferBatchSizeDefaultAs256 <T> {
}
