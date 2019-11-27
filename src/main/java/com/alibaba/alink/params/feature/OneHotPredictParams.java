package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;

/**
 * parameters of one hot predictor.
 */
public interface OneHotPredictParams<T> extends WithParams<T>,
	HasReservedCols <T>,
	HasOutputCol <T> {
}
