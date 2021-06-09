package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * parameters of naive bayes predictor.
 */
public interface NaiveBayesTextPredictParams<T> extends
	RichModelMapperParams <T>,
	HasVectorCol <T> {
}
