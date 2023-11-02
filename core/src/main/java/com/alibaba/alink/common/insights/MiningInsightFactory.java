package com.alibaba.alink.common.insights;

public class MiningInsightFactory {

	public static CorrelationInsightBase getMiningInsight(InsightType type, Subject subject) {
		switch (type) {
			case Correlation:
				return new CorrelationInsight(subject, type);
			case CrossMeasureCorrelation:
				return new CrossMeasureCorrelationInsight(subject, type);
			case Clustering2D:
				return new ScatterplotClusteringInsight(subject, type);
			default:
				return null;
		}
	}
}
