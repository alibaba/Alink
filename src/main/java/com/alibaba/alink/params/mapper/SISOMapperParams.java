package com.alibaba.alink.params.mapper;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameters for SISOMapper.
 */
public interface SISOMapperParams<T> extends
	HasSelectedCol<T>,
	HasOutputColDefaultAsNull<T>,
	HasReservedCols<T> {
}
