package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.dataproc.tensor.HasTensorShape;
import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.shared.HasHandleInvalid;

/**
 * Parameters of transforming to tensor.
 */
public interface ToTensorParams<T> extends
	SISOMapperParams <T>,
	HasTensorShape <T>,
	HasHandleInvalid <T>,
	HasTensorDataType <T> {

}