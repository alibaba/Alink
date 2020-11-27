package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface NumericalTypeCastParams<T> extends
	HasSelectedCols <T>,
	HasOutputColsDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T>,
	HasTargetType <T> {
}
