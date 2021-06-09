package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;

/**
 * Parameter of imputer predict for vector data.
 */
public interface VectorImputerPredictParams<T> extends
	ModelMapperParams <T>,
	HasOutputColDefaultAsNull <T> {
}
