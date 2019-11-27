package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.operator.common.distance.*;

import java.io.Serializable;

/**
 * Various distance types.
 */
public enum DistanceType implements Serializable {
	/**
	 * EUCLIDEAN
	 */
	EUCLIDEAN(new EuclideanDistance()),
	/**
	 * COSINE
	 */
	COSINE(new CosineDistance()),
	/**
	 * CITYBLOCK
	 */
	CITYBLOCK(new ManHattanDistance()),
	/**
	 * HAVERSINE
	 */
	HAVERSINE(new HaversineDistance()),

	/**
	 * JACCARD
	 */
	JACCARD(new JaccardDistance());

	public ContinuousDistance getContinuousDistance() {
		return continuousDistance;
	}

	private ContinuousDistance continuousDistance;

	DistanceType(ContinuousDistance continuousDistance){
		this.continuousDistance = continuousDistance;
	}


}
