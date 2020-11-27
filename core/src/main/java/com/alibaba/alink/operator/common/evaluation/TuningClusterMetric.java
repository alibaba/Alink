package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;

public enum TuningClusterMetric {
	SSB(ClusterMetrics.SSB),
	SSW(ClusterMetrics.SSW),
	VRC(ClusterMetrics.VRC),
	SP(ClusterMetrics.SP),
	CP(ClusterMetrics.CP),
	DB(ClusterMetrics.DB),
	NMI(ClusterMetrics.NMI),
	PURITY(ClusterMetrics.PURITY),
	RI(ClusterMetrics.RI),
	ARI(ClusterMetrics.ARI),
	SILHOUETTE_COEFFICIENT(ClusterMetrics.SILHOUETTE_COEFFICIENT);

	private ParamInfo <Double> metricKey;

	TuningClusterMetric(ParamInfo <Double> metricKey) {
		this.metricKey = metricKey;
	}

	public ParamInfo <Double> getMetricKey() {
		return metricKey;
	}
}
