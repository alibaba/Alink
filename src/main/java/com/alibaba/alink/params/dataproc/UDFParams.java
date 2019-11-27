package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface UDFParams<T> extends
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasReservedCols <T> {
}
