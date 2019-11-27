package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface UDTFParams<T> extends
	HasSelectedCols <T>,
	HasOutputCols<T>,
	HasReservedCols <T> {
}
