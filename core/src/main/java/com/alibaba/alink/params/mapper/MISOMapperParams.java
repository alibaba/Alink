package com.alibaba.alink.params.mapper;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Parameters for MISOMapper.
 *
 * @param <T>
 */
public interface MISOMapperParams<T> extends
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasReservedColsDefaultAsNull <T> {
}
