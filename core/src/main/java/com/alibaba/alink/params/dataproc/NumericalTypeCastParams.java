package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface NumericalTypeCastParams<T> extends
	HasSelectedCols <T>,
	HasOutputColsDefaultAsNull <T>,
	HasReservedCols <T>,
	HasTargetType <T> {
}
