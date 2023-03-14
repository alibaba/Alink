package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.HaversineDistance;
import com.alibaba.alink.operator.common.distance.JaccardDistance;
import com.alibaba.alink.operator.common.distance.ManHattanDistance;

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

	public FastDistance getFastDistance() {
		return fastDistance;
	}

	private FastDistance fastDistance;

	DistanceType(FastDistance fastDistance) {
		this.fastDistance = fastDistance;
	}

}
