package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of ridge regression predictor.
 */
public interface RidgeRegPredictParams<T> extends
	RegPredictParams <T>,
	HasVectorColDefaultAsNull <T> {
}
