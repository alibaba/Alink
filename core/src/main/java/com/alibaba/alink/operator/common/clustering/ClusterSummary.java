package com.alibaba.alink.operator.common.clustering;

import java.io.Serializable;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class ClusterSummary implements Serializable {
	private static final long serialVersionUID = 8631961988528403010L;
	public String center;
	public Long clusterId;
	public Double weight;
	public String type;

	public ClusterSummary(String center, Long clusterId, Double weight, String type) {
		this.center = center;
		this.clusterId = clusterId;
		this.weight = weight;
		this.type = type;
	}

	public ClusterSummary(String center, Long clusterId, Double weight) {
		this(center, clusterId, weight, null);
	}

	public static ClusterSummary deserialize(String json) {
		return gson.fromJson(json, ClusterSummary.class);
	}

	public String serialize() {
		return gson.toJson(this, ClusterSummary.class);
	}
}
