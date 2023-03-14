package com.alibaba.alink.operator.common.clustering.dbscan;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;

public class DbscanModelTrainData {
	/**
	 * predict scope (after loading, before predicting)
	 */
	public double epsilon;
	public String vectorColName;
	public HasClusteringDistanceType.DistanceType distanceType;

	/**
	 * Tuple3: clusterId, clusterCentroid
	 */
	public Iterable <Tuple2 <Vector, Long>> coreObjects;

}
