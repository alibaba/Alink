package com.alibaba.alink.operator.common.clustering.kmeans;

/**
 * InitMode for Kmeans.
 */
public enum InitMode {
	/**
	 * Random init.
	 */
	RANDOM,
	/**
	 * KMeansPlusPlus init.
	 */
	K_MEANS_PARALLEL
}
