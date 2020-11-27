package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params of QuantileDiscretizerPredict.
 */
public interface QuantileDiscretizerPredictParams<T> extends
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T>,
	HasHandleInvalid <T>,
	HasEncodeWithoutWoeDefaultAsIndex <T>,
	HasDropLast <T>, HasNumThreads <T> {
}
