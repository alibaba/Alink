package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Parameters of naive bayes predict process.
 */
public interface NaiveBayesPredictParams<T> extends
	RichModelMapperParams <T>,
	HasNumThreads <T> {
}
