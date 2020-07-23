package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Trait for parameter PcaPredict.
 */
public interface PcaPredictParams<T> extends
    HasReservedCols<T>,
    HasPredictionCol<T>,
    HasVectorColDefaultAsNull<T> {
}
