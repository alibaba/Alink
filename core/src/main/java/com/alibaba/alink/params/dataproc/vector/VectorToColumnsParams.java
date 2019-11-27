package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * parameters of vector to columns.
 */
public interface VectorToColumnsParams<T> extends
	HasSelectedCol <T>,
	HasOutputCols <T>,
	HasReservedCols <T> {}