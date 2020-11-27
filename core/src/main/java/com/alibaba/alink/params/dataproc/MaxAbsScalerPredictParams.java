package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Parameter of MaxAbsScaler predict.
 */
public interface MaxAbsScalerPredictParams<T> extends
	SrtPredictMapperParams <T>, HasNumThreads <T> {
}
