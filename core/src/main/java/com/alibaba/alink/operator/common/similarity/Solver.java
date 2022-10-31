package com.alibaba.alink.operator.common.similarity;

import com.alibaba.alink.operator.common.similarity.dataConverter.KDTreeModelDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.LSHModelDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.LocalLSHModelDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;

/**
 * Solver for ApproxVectorSimilarityTop.
 */
public enum Solver {
	/**
	 * KDTree approximation.
	 */
	KDTREE(new KDTreeModelDataConverter()),
	/**
	 * LSH approximation.
	 */
	LSH(new LSHModelDataConverter()),

	/**
	 * LSH approximation for local operator.
	 */
	LOCAL_LSH(new LocalLSHModelDataConverter());

	private NearestNeighborDataConverter dataConverter;

	Solver(NearestNeighborDataConverter dataConverter) {
		this.dataConverter = dataConverter;
	}

	public NearestNeighborDataConverter getDataConverter() {
		return dataConverter;
	}
}
