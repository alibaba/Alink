package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * Parameters for Gaussian Mixture Model prediction.
 *
 * @param <T> The class that implement this interface.
 */
public interface GmmPredictParams<T> extends
	RichModelMapperParams <T>,
	HasVectorCol <T> {
}
