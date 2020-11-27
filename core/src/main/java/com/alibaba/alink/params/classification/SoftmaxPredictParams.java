package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * parameters of softmax predictor.
 */
public interface SoftmaxPredictParams<T> extends
	LinearModelMapperParams <T>, HasNumThreads <T> {
}
