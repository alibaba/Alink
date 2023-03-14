package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface PandasUdfParams<T> extends
	MapperParams <T>,
	HasSelectedCols <T>,
	HasOutputCols <T>,
	HasReservedColsDefaultAsNull <T>,
	HasUdfFilePath <T>,
	HasFunctionParams <T>,
	HasEntryFunction <T> {
}
