package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.dataproc.HasMax;
import com.alibaba.alink.params.dataproc.HasMin;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameter of  MinMaxScaler train for vector data.
 */
public interface VectorMinMaxScalerTrainParams<T> extends WithParams<T>,
	HasSelectedCol <T>,
	HasMin <T>,
	HasMax <T> {
}
