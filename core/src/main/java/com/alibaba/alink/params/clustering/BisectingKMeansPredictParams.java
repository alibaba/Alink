package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.mapper.RichModelMapperParams;

/**
 * Params for BisectingKMeansPrediction.
 */
public interface BisectingKMeansPredictParams<T> extends WithParams<T>,
	RichModelMapperParams<T> {
}
