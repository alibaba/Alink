package com.alibaba.alink.params.mapper;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameters for SISOMapper.
 */
public interface SISOMapperParams<T> extends
	MapperParams <T>,
	HasSelectedCol <T>,
	HasOutputColDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T> {
}
