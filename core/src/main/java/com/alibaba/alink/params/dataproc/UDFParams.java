package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.udf.HasFuncName;

public interface UDFParams<T> extends
	HasFuncName<T>,
	HasSelectedCols<T>,
	HasOutputCol<T>,
	HasReservedCols<T> {
}
