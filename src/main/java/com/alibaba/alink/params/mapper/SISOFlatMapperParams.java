package com.alibaba.alink.params.mapper;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Params for SISOFlatMapper, include selectedCol, outputCol and reservedCol.
 */
public interface SISOFlatMapperParams<T> extends
	HasSelectedCol <T>,
	HasOutputColDefaultAsNull <T>,
	HasReservedCols <T> {
}
