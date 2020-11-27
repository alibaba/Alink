package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * parameters of logistic regression predictor.
 */
public interface LogisticRegressionPredictParams<T> extends
	LinearModelMapperParams <T>, HasNumThreads <T> {
}
