package com.alibaba.alink.operator.common.clustering.dbscan;

import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;

import java.util.List;

public class DbscanModelPredictData {
	/**
	 * predict scope (after loading, before predicting)
	 */
	public double epsilon;
	public FastDistance baseDistance;
	public String vectorColName;
	public DistanceType distanceType;

	/**
	 * Tuple3: clusterId, clusterCentroid
	 */
	public List <FastDistanceVectorData> coreObjects;

}
