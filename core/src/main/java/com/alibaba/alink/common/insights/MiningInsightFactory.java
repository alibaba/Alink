package com.alibaba.alink.common.insights;

public class MiningInsightFactory {

	public static CorrelationInsightBase getMiningInsight(Insight insight) {
		switch (insight.type) {
			case Correlation:
				return new CorrelationInsight(insight);
			case CrossMeasureCorrelation:
				return new CrossMeasureCorrelationInsight(insight);
			case Clustering2D:
				return new ScatterplotClusteringInsight(insight);
			default:
				return null;
		}
	}
}
