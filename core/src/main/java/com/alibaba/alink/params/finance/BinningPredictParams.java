package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasDropLast;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * BinningPredictParams
 */
public interface BinningPredictParams<T> extends
	ModelMapperParams <T>,
	HasSelectedCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColsDefaultAsNull <T>,
	HasDefaultWoe <T>,
	HasHandleInvalid <T>,
	HasEncode <T>,
	HasDropLast <T> {
}
