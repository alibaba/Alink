package com.alibaba.alink.params.clustering;

/**
 * Params for KMeans4LongiLatitudeTrainer.
 */
public interface GeoKMeansTrainParams<T> extends
	BaseKMeansTrainParams <T>,
	HasLatitudeCol <T>,
	HasLongitudeCol <T> {
}
