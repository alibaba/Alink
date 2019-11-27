package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasIndices;

/**
 * parameters of vector slicer.
 */
public interface VectorSliceParams<T> extends
	SISOMapperParams<T>,
	HasIndices <T> {}