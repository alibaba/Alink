package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.mapper.SISOModelMapperParams;

/**
 * Parameters for {@link com.alibaba.alink.pipeline.dataproc.StringIndexerModel}.
 */
public interface SparseFeatureIndexerPredictParams<T> extends
	SISOModelMapperParams <T>,
	HasHandleDuplicateFeature <T> {
}
