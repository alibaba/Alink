package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;

/**
 * Parameter of standard predict for vector data.
 */
public interface VectorStandardPredictParams<T> extends
	MapperParams <T>,
	HasOutputColDefaultAsNull <T> {
}
