package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * parameters of linear svm predictor.
 */
public interface LinearSvmPredictParams<T> extends
	LinearModelMapperParams <T>, HasNumThreads <T> {
}
