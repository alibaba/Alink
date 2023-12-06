package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.List;

public class CrossMeasureCorrelationInsight extends CorrelationInsightBase {

	public CrossMeasureCorrelationInsight(Insight insight) {
		super(insight);
	}

	public LocalOperator <?>[] preprocess(LocalOperator <?>... sources) {
		LocalOperator <?>[] sourceInput = new LocalOperator[2];
		if (sources.length == 1) {
			LocalOperator <?> input = sources[0];
			if (needFilter) {
				input = Mining.filter(input, insight.subject.subspaces);
			}
			if (needGroup) {
				List <LocalOperator <?>> result = groupData(input, insight.subject);
				sourceInput[0] = result.get(0);
				sourceInput[1] = result.get(1);
			}
		} else {
			sourceInput = sources;
		}
		return sourceInput;
	}

	@Override
	public Insight processData(LocalOperator <?>... sources) {
		if (insight.subject.measures.size() != 2) {
			insight.score = 0;
			return insight;
		}
		MeasureAggr aggr1 = insight.subject.measures.get(0).aggr;
		MeasureAggr aggr2 = insight.subject.measures.get(1).aggr;
		if (aggr1.equals(MeasureAggr.COUNT) || aggr2.equals(MeasureAggr.COUNT) || !aggr1.equals(aggr2)) {
			insight.score = 0;
			return insight;
		}
		LocalOperator <?>[] sourceInput = preprocess(sources);
		insight.score = computeScore(sourceInput);
		this.fillLayout();
		return insight;
	}

	@Override
	public void fillLayout() {
		this.insight.layout.xAxis = this.insight.subject.measures.get(0).aggr + "("
			+ this.insight.subject.measures.get(0).colName + ")";
		this.insight.layout.yAxis = this.insight.subject.measures.get(1).aggr + "("
			+ this.insight.subject.measures.get(1).colName + ")";
		this.insight.layout.title = insight.layout.xAxis + " and " + insight.layout.yAxis + " correlation";
		StringBuilder builder = new StringBuilder();
		if (null != insight.subject.subspaces && !insight.subject.subspaces.isEmpty()) {
			builder.append(insight.getSubspaceStr(insight.subject.subspaces)).append(" 条件下，");
		}
		builder.append(this.insight.layout.xAxis).append(" 与 ").append(this.insight.layout.yAxis).append(" 存在相关性");
		this.insight.layout.description = builder.toString();
	}

}