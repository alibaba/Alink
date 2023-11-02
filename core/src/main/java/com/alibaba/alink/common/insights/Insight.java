package com.alibaba.alink.common.insights;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;

public class Insight implements Serializable {

	public Subject subject;
	public InsightType type;
	public double score;
	public BaseLayout layout;

	public Insight() {}

	@Override
	public String toString() {
		return new StringBuilder()
			.append("score: ").append(score).append(", \ttype: ").append(type.name())
			.append("\n|-subspaces: ").append(JsonConverter.toJson(subject.subspaces))
			.append("\n|-breakdown: ").append(JsonConverter.toJson(subject.breakdown))
			.append("\n|-measures: ").append(JsonConverter.toJson(subject.measures))
			//.append("\n|-layout: ").append((null==layout)?"null":JsonConverter.toJson(layout))
			.toString();
	}

	public static InsightType[] singlePointInsightType() {
		return new InsightType[] {
			InsightType.Attribution,
			InsightType.Evenness,
			InsightType.OutstandingNo1,
			InsightType.OutstandingLast,
			InsightType.OutstandingTop2};
	}

	public static InsightType[] singleShapeInsightType() {
		return new InsightType[] {
			InsightType.ChangePoint,
			InsightType.Outlier,
			InsightType.Seasonality,
			InsightType.Trend};
	}

	public static InsightType[] compoundInsightType() {
		return new InsightType[] {
			InsightType.Correlation,
			InsightType.CrossMeasureCorrelation,
			InsightType.Clustering2D};
	}
}
