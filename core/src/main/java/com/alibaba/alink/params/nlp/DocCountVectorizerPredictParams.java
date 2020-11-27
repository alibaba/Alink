package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.mapper.SISOModelMapperParams;
import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Params for DocCountVectorizerPredict.
 */
public interface DocCountVectorizerPredictParams<T> extends
	SISOModelMapperParams <T>, HasNumThreads <T> {
}
