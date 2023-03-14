package com.alibaba.alink.operator.common.clustering.kmodes;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

public class KModesModelData {
	public List <Tuple3 <Long, Double, String[]>> centroids;
	public String[] featureColNames;
}
