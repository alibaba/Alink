package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.MISOMapperParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;

/**
 * parameters of vector assembler.
 */
public interface VectorAssemblerParams<T> extends
	MISOMapperParams <T>,
	HasHandleInvalid <T> {
}