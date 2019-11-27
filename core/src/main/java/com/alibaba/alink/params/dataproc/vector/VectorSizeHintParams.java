package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.HasSize;

/**
 * parameters of vector size hint.
 */
public interface VectorSizeHintParams<T> extends
	SISOMapperParams<T>,
	HasSize <T>,
	HasHandleInvalid <T> {}