package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of fm predictor.
 */
public interface FmPredictParams<T> extends
    HasVectorColDefaultAsNull<T>,
	HasReservedColsDefaultAsNull<T>,
    HasPredictionCol<T>,
    HasPredictionDetailCol<T> {
}
