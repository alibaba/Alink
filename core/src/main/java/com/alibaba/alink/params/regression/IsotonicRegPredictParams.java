package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;

/**
 * Params for IsotonicRegressionPredictor.
 */
public interface IsotonicRegPredictParams<T>
	extends HasPredictionCol <T>, HasNumThreads <T> {
}
