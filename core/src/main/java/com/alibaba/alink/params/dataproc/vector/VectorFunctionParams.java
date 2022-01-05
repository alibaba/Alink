package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasFuncName;
import com.alibaba.alink.params.shared.HasWithVariable;

public interface VectorFunctionParams<T> extends
	SISOMapperParams <T>,
	HasFuncName <T>,
	HasWithVariable <T> {}