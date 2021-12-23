package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

public interface ExpandExtendedVarsParams<T> extends
	HasVectorCol <T>,
	HasNumVars <T>,
	HasExtendedVectorColDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputCol <T> {
}
