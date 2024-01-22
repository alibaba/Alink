package com.alibaba.alink.common.insights;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Insight implements Serializable {

	public Subject subject;
	public InsightType type;
	public double score;
	public double originScore;
	public LayoutData layout;

	public List <Subspace> attachSubspaces;

	public Insight() {
		this.attachSubspaces = new ArrayList <>();
	}

	public Insight addAttachSubspace(Subspace subspace) {
		if (null == this.attachSubspaces) {
			attachSubspaces = new ArrayList <>();
		}
		this.attachSubspaces.add(subspace);
		return this;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("score: ").append(score).append(", \ttype: ").append(type.name());
		stringBuilder.append("\n|-origin score: ").append(originScore);
		if (null != subject) {
			if (null != subject.subspaces && subject.subspaces.size() > 0) {
				stringBuilder.append("\n|-subspaces: ").append(JsonConverter.toJson(subject.subspaces));
			}
			if (null != subject.breakdown) {
				stringBuilder.append("\n|-breakdown: ").append(JsonConverter.toJson(subject.breakdown));
			}
			if (null != subject.measures && subject.measures.size() > 0) {
				stringBuilder.append("\n|-measures: ").append(JsonConverter.toJson(subject.measures));
			}
		}
		if (null != this.attachSubspaces && this.attachSubspaces.size() > 0) {
			stringBuilder.append("\n|-attach subspaces: ").append(JsonConverter.toJson(this.attachSubspaces));
		}
		if (layout != null) {
			if (layout.description != null) {
				stringBuilder.append("\n|-description: ").append(layout.description);
			}
			if (layout.title != null) {
				stringBuilder.append("\n|-title: ").append(layout.title);
			}
			if (layout.focus != null) {
				stringBuilder.append("\n|-focus: ").append(layout.focus);
			}
			if (layout.xAxis != null) {
				stringBuilder.append("\n|-xAxis: ").append(layout.xAxis);
			}
			if (layout.yAxis != null) {
				stringBuilder.append("\n|-yAxis: ").append(layout.yAxis);
			}
			if (layout.lineA != null) {
				stringBuilder.append("\n|-lineA: ").append(layout.lineA);
			}
			if (layout.lineB != null) {
				stringBuilder.append("\n|-lineB: ").append(layout.lineB);
			}
		}
		return stringBuilder.toString();
	}

	public static InsightType[] singlePointInsightType() {
		return new InsightType[] {
			InsightType.Attribution,
			InsightType.Evenness,
			InsightType.OutstandingNo1,
			InsightType.OutstandingLast,
			InsightType.OutstandingTop2
		};
	}

	public static InsightType[] singleShapeInsightType() {
		return new InsightType[] {
			InsightType.ChangePoint,
			InsightType.Outlier,
			InsightType.Seasonality,
			InsightType.Trend
		};
	}

	public static InsightType[] compoundInsightType() {
		return new InsightType[] {
			InsightType.Correlation,
			InsightType.CrossMeasureCorrelation,
			InsightType.Clustering2D};
	}

	public static InsightType[] measureCorrInsightType() {
		return new InsightType[] {
			InsightType.CrossMeasureCorrelation,
			InsightType.Clustering2D};
	}

	public String getSubspaceStr(List <Subspace> subspaces) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < subspaces.size(); i++) {
			Subspace subspace = subspaces.get(i);
			builder.append(subspace.colName).append("=").append(subspace.value);
			if (i != subspaces.size() - 1) {
				builder.append(" AND ");
			}
		}
		return builder.toString();
	}

	public String getTitle() {
		String title = String.format("%s里%s的%s",
			subject.breakdown.colName, subject.measures.get(0).colName, subject.measures.get(0).aggr.getCnName());
		if (!subject.subspaces.isEmpty()) {
			title = subject.subspaces.get(0).strInDescription() + title;
		}
		return title;
	}
}
