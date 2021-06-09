package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Params for MultiHot predict.
 */
public interface MultiHotPredictParams<T> extends
	ModelMapperParams <T>,
    HasSelectedCols<T>,
    HasReservedColsDefaultAsNull<T>,
	HasOutputCols <T>,
	HasHandleInvalid <T>,
	HasEncodeWithoutWoeAndIndex<T> {
}