package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.MISOMapperParams;
import com.alibaba.alink.params.shared.HasBiFuncName;

public interface VectorBiFunctionParams<T> extends
	MISOMapperParams <T>,
	HasBiFuncName <T> {}