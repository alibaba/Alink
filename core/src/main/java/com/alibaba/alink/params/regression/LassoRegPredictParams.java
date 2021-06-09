package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of lasso regression predictor.
 */
public interface LassoRegPredictParams<T> extends
	RegPredictParams <T>,
	HasVectorColDefaultAsNull <T> {
}
