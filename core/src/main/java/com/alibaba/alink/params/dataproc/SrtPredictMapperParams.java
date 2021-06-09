package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;

/**
 * Parameter of SrtModelMapper.
 */
public interface SrtPredictMapperParams<T> extends
	ModelMapperParams <T>,
	HasOutputColsDefaultAsNull <T> {
}
