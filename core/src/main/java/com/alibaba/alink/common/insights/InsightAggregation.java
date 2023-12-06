package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class InsightAggregation {
	private List <Insight> insights = new ArrayList <>();

	private HashMap <ConstraintType, Integer> constraintMap = new HashMap <>();
	private List<Tuple2<ConstraintType, Integer>> sortConsMapList = new ArrayList <>();
	private HashMap<String, List<Integer>> indexList = new HashMap <>();
	private List<List<String>> insightKeyNames = new ArrayList <>();

	public enum ConstraintType {
		BreakdownMeasureColAndType,
		BreakdownMeasureCol,
		Breakdown,
		InsightBreakdownMeasureColAndType,
		InsightBreakdownMeasureCol,
		InsightBreakdown,
		Insight
	}

	public InsightAggregation () {
		constraintMap.put(ConstraintType.BreakdownMeasureColAndType, 2);
		constraintMap.put(ConstraintType.BreakdownMeasureCol, 4);
		constraintMap.put(ConstraintType.Breakdown, 10);
		constraintMap.put(ConstraintType.InsightBreakdownMeasureColAndType, 1);
		constraintMap.put(ConstraintType.InsightBreakdownMeasureCol, 2);
		constraintMap.put(ConstraintType.InsightBreakdown, 4);
		constraintMap.put(ConstraintType.Insight, 5);
	}

	public InsightAggregation sortConstraint() {
		sortConsMapList.clear();
		constraintMap.entrySet().stream().sorted((k1, k2) -> k1.getValue().compareTo(k2.getValue()))
			.forEach(k -> sortConsMapList.add(Tuple2.of(k.getKey(), k.getValue())));
		return this;
	}

	private String getKey(Insight insight, ConstraintType type) {
		Tuple2 <String, String> value;
		switch (type) {
			case Insight:
				return "insight_" + insight.type.toString();
			case InsightBreakdown:
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName;
			case InsightBreakdownMeasureCol:
				value = getMeasureKey(insight.subject.measures);
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName + ";" + value.f0;
			case InsightBreakdownMeasureColAndType:
				value = getMeasureKey(insight.subject.measures);
				return "insight_" + insight.type.toString() + ";breakdown_" + insight.subject.breakdown.colName + ";" + value.f1;
			case BreakdownMeasureColAndType:
				value = getMeasureKey(insight.subject.measures);
				return "breakdown_" + insight.subject.breakdown.colName + ";" + value.f1;
			case BreakdownMeasureCol:
				value = getMeasureKey(insight.subject.measures);
				return "breakdown_" + insight.subject.breakdown.colName + ";" + value.f0;
			case Breakdown:
				return "breakdown_" + insight.subject.breakdown.colName;
			default:
				return "";
		}
	}

	public InsightAggregation addConstraint(ConstraintType type, int num) {
		constraintMap.put(type, num);
		return this;
	}

	public InsightAggregation removeConstraint(ConstraintType type) {
		if (constraintMap.containsKey(type)) {
			constraintMap.remove(type);
		}
		return this;
	}

	private Tuple2 <String, String> getMeasureKey(List <Measure> measures) {
		StringBuilder measureBuilder = new StringBuilder();
		StringBuilder measureTypeBuilder = new StringBuilder();
		for (int i = 0; i < measures.size(); i++) {
			measureBuilder.append("measure_");
			measureBuilder.append(measures.get(i).colName);
			measureTypeBuilder.append("measure_").append(measures.get(i).colName);
			measureTypeBuilder.append("_type_").append(measures.get(i).aggr.toString());
			if (i < measures.size() - 1) {
				measureBuilder.append(";");
				measureTypeBuilder.append(";");
			}
		}
		return Tuple2.of(measureBuilder.toString(), measureTypeBuilder.toString());
	}

	public InsightAggregation addInsight(Insight insight) {
		if (this.sortConsMapList.size() == 0) {
			sortConstraint();
		}
		List<String> keyList = new ArrayList <>();
		for (int i = 0; i < sortConsMapList.size(); i++) {
			keyList.add(getKey(insight, sortConsMapList.get(i).f0));
		}
		boolean add = true;
		int replaceIndex = -1;
		for (int i = 0; i < sortConsMapList.size(); i++) {
			ConstraintType type = sortConsMapList.get(i).f0;
			String keyName = keyList.get(i);
			// 不存在，或者未达到上限
			if (!this.indexList.containsKey(keyName) || this.indexList.get(keyName).size() < this.constraintMap.get(type)) {
				continue;
			} else {
				add = false;
				double minScore = 1.0;
				int minIndex = -1;
				for (Integer index : this.indexList.get(keyName)) {
					if (insights.get(index).score < minScore) {
						minScore = insights.get(index).score;
						minIndex = index;
					}
				}
				if (insight.score > minScore) {
					replaceIndex = minIndex;
					break;
				}
			}
		}
		if (add) {
			insights.add(insight);
			for (String keyName : keyList) {
				if (!indexList.containsKey(keyName)) {
					indexList.put(keyName, new ArrayList <>());
				}
				indexList.get(keyName).add(insights.size() - 1);
			}
			insightKeyNames.add(keyList);
		} else if (replaceIndex > 0) {
			insights.set(replaceIndex, insight);
			List<String> oldKeyNames = insightKeyNames.set(replaceIndex, keyList);
			for (String keyName : oldKeyNames) {
				indexList.get(keyName).remove(replaceIndex);
			}
			for (String keyName : keyList) {
				indexList.get(keyName).add(replaceIndex);
			}
		}
		return this;
	}

	public List<Insight> getInsights() {
		return insights.stream().sorted(Comparator.comparingDouble(insight -> 0 - insight.score)).collect(Collectors.toList());
	}
}
