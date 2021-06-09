package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of fm predictor.
 */
public interface FmPredictParams<T> extends
	RichModelMapperParams <T>,
	HasVectorColDefaultAsNull <T> {
}
