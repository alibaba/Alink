package com.alibaba.alink.operator.common.clustering.dbscan;

import java.io.Serializable;

public class LocalCluster implements Serializable {
	private static final long serialVersionUID = 1286966569356492688L;
	private int[] keys;
	private int[] clusterIds;

	public int[] getKeys() {
		return keys;
	}

	public int[] getClusterIds() {
		return clusterIds;
	}

	public LocalCluster(int[] keys, int[] clusterIds) {
		this.keys = keys;
		this.clusterIds = clusterIds;
	}
}
