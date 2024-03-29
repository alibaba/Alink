package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasDegreeDefaultAs2;

/**
 * parameters of vector polynomial expand.
 */
public interface VectorPolynomialExpandParams<T> extends
	SISOMapperParams <T>,
	HasDegreeDefaultAs2 <T> {}