package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of linear regression predictor.
 */
public interface LinearRegPredictParams<T> extends
	HasReservedColsDefaultAsNull <T>,
	HasPredictionCol <T>,
	HasVectorColDefaultAsNull <T>, HasNumThreads <T> {
}
