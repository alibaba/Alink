package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.List;

public class CorrelationInsight extends CorrelationInsightBase {

	public CorrelationInsight(Insight insight) {
		super(insight);
	}

	@Override
	public Insight processData(LocalOperator <?>... sources) {
		LocalOperator <?>[] sourceInput = new LocalOperator[]{sources[0], sources[1]};
		if (needFilter) {
			sourceInput[0] = Mining.filter(sourceInput[0], this.insight.subject.subspaces);
			sourceInput[1] = Mining.filter(sourceInput[1], this.insight.attachSubspaces);
		}
		if (needGroup) {
			sourceInput[0] = groupData(sourceInput[0], insight.subject).get(0);
			sourceInput[1] = groupData(sourceInput[1], insight.subject).get(0);
		}
		insight.score = computeScore(sourceInput);
		this.fillLayout();
		return this.insight;
	}

	@Override
	public void fillLayout() {
		List <Measure> measures = this.insight.subject.measures;
		this.insight.layout.xAxis = this.insight.subject.breakdown.colName;
		this.insight.layout.yAxis = measures.get(0).aggr + "(" + measures.get(0).colName + ")";
		this.insight.layout.lineA = insight.getSubspaceStr(insight.subject.subspaces);
		this.insight.layout.lineB = insight.getSubspaceStr(insight.attachSubspaces);
		this.insight.layout.title = "子集的统计指标 " +
			String.format("%s的%s", measures.get(0).colName, measures.get(0).aggr.getCnName())
			+ " 存在相关性";
		StringBuilder builder = new StringBuilder();
		builder.append(insight.layout.lineA).append(" 与 ").append(insight.layout.lineB).append(" 条件下，");
		builder.append("统计指标 ")
			.append(String.format("%s的%s", measures.get(0).colName, measures.get(0).aggr.getCnName()))
			.append(" 存在相关性。");
		//if (this.range.intValue() > MAX_SCALAR_THRESHOLD.intValue()) {
		//	builder.append("*由于二者数值范围差异较大，对第二条线进行了缩放");
		//}
		this.insight.layout.description = builder.toString();
	}

}
