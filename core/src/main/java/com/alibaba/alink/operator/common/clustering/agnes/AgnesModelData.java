package com.alibaba.alink.operator.common.clustering.agnes;

import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;

import java.util.List;

public class AgnesModelData {
	public int k;
	public double distanceThreshold;
	public HasClusteringDistanceType.DistanceType distanceType;
	public Linkage linkage;
	public String[] featureColNames;
	public String idCol;

	public List <AgnesSample> centroids;
}
