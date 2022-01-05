package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;

/**
 * Parameters of transforming to MTable.
 */
public interface ToMTableParams<T> extends
	SISOMapperParams <T>,
	HasHandleInvalid <T> {
}