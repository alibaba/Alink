package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.udf.HasFuncName;

public interface UDTFParams<T> extends
	HasFuncName <T>,
	HasSelectedCols <T>,
	HasOutputCols <T>,
	HasReservedColsDefaultAsNull <T> {
}
