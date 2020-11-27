package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Params for DocHashCountVectorizerPredict.
 */
public interface DocHashCountVectorizerPredictParams<T> extends
	DocCountVectorizerPredictParams <T>, HasNumThreads <T> {
}
