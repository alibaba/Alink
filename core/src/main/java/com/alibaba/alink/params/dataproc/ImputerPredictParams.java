package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Parameter of Imputer predict.
 */
public interface ImputerPredictParams<T>
	extends SrtPredictMapperParams <T>, HasNumThreads <T> {
}
