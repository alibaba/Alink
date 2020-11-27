package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * parameters of one hot predictor.
 */
public interface OneHotPredictParams<T> extends
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T>,
	HasHandleInvalid <T>,
	HasEncodeWithoutWoe <T>,
	HasDropLast <T>, HasNumThreads <T> {
}
