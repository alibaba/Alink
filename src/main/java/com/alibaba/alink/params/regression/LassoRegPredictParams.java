package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of lasso regression predictor.
 *
 */
public interface LassoRegPredictParams<T> extends
	HasReservedCols <T>,
	HasPredictionCol <T>,
	HasVectorColDefaultAsNull <T> {
}
