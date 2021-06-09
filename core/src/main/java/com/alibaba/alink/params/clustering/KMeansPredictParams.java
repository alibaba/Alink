package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.mapper.RichModelMapperParams;

/**
 * Params for KMeansPredictor.
 */
public interface KMeansPredictParams<T> extends
	RichModelMapperParams <T>,
	HasPredictionDistanceCol <T> {
}
