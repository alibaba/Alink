package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.shared.colname.*;

/**
 * parameters of one hot predictor.
 */
public interface OneHotPredictParams<T> extends
	HasSelectedCols<T>,
	HasReservedCols<T>,
	HasOutputColsDefaultAsNull<T>,
	HasHandleInvalid<T>,
	HasEncode<T>,
	HasDropLast<T> {
}
