package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface BasePyTableFnParams<T> extends
	HasResultTypes <T>,
	HasSelectedCols <T>,
	HasOutputCols <T>,
	HasReservedColsDefaultAsNull <T> {
}
