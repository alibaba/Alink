package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Parameter of  MinMaxScaler predict.
 */
public interface MinMaxScalerPredictParams<T> extends WithParams <T>,
	SrtPredictMapperParams <T>, HasNumThreads <T> {
}
