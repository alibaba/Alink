package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of linear regression predictor.
 */
public interface LinearRegPredictParams<T> extends
	RegPredictParams <T>,
	HasVectorColDefaultAsNull <T> {
}
