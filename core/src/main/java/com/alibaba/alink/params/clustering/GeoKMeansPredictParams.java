package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasNumThreads;
/**
 * Params for KMeans4LongiLatitudePredictor.
 *
 * @param <T>
 */
public interface GeoKMeansPredictParams<T> extends WithParams <T>,
	KMeansPredictParams <T>, HasNumThreads <T> {
}
