package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;

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
		this.insight.layout.xAxis = this.insight.subject.breakdown.colName;
		this.insight.layout.yAxis = insight.subject.measures.get(0).aggr + "(" + insight.subject.measures.get(0).colName + ")";
		this.insight.layout.lineA = insight.getSubspaceStr(insight.subject.subspaces);
		this.insight.layout.lineB = insight.getSubspaceStr(insight.attachSubspaces);
		this.insight.layout.title = insight.layout.yAxis + " breakdown by " + insight.layout.xAxis;
		StringBuilder builder = new StringBuilder();
		builder.append(insight.layout.lineA).append(" 与 ").append(insight.layout.lineB).append("条件下，");
		builder.append("统计指标 ").append(this.insight.layout.yAxis).append(" 存在相关性。");
		//if (this.range.intValue() > MAX_SCALAR_THRESHOLD.intValue()) {
		//	builder.append("*由于二者数值范围差异较大，对第二条线进行了缩放");
		//}
		this.insight.layout.description = builder.toString();
	}

}
