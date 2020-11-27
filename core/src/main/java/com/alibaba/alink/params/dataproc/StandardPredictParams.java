package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Parameter of standard predict.
 */
public interface StandardPredictParams<T>
	extends SrtPredictMapperParams <T>, HasNumThreads <T> {
}
