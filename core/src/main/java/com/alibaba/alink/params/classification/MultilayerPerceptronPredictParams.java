package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface MultilayerPerceptronPredictParams<T> extends
	RichModelMapperParams<T>, HasVectorColDefaultAsNull <T> {
}
