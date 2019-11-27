package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameter of  MinMaxScaler predict.
 */
public interface MinMaxScalerPredictParams<T> extends WithParams<T>,
	SrtPredictMapperParams <T> {
}
