package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * Parameters for Gaussian Mixture Model prediction.
 *
 * @param <T> The class that implement this interface.
 */
public interface GmmPredictParams<T> extends WithParams <T>,
	HasVectorCol <T>,
	RichModelMapperParams <T>, HasNumThreads <T> {
}
