package com.alibaba.alink.operator.common.clustering;

/**
 * the result of findCluster function
 *
 * @author guotao.gt
 */
public class FindResult {
	private Long clusterId;
	private Double distance;

	public FindResult(Long clusterId, Double distance) {
		this.clusterId = clusterId;
		this.distance = distance;
	}

	public Long getClusterId() {
		return clusterId;
	}

	public Double getDistance() {
		return distance;
	}
}
