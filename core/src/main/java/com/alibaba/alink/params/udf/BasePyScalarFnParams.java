package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface BasePyScalarFnParams<T> extends
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasResultType <T>,
	HasReservedColsDefaultAsNull <T> {
}
