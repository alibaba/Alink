package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;

/**
 * Parameter of MinMaxScaler predict for vector data.
 */
public interface VectorMinMaxScalerPredictParams<T> extends WithParams<T>,
	HasOutputColDefaultAsNull <T> {
}
