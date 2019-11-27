package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface QuantileDiscretizerPredictParams<T> extends
	HasSelectedCols <T>,
	HasReservedCols <T>,
	HasOutputColsDefaultAsNull <T> {
}
