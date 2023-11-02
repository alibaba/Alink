package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;
import org.apache.commons.math3.distribution.TDistribution;

import java.util.HashMap;
import java.util.Map.Entry;

public class CorrelationInsight extends CorrelationInsightBase {
	private Subject subject2;

	public CorrelationInsight(Subject subject, InsightType type) {
		super(subject, type);
	}

	public CorrelationInsight setSubject2(Subject subject) {
		this.subject2 = subject;
		return this;
	}

	@Override
	public Insight processData(LocalOperator <?>... sources) {
		LocalOperator <?>[] sourceInput = new LocalOperator[]{sources[0], sources[1]};
		if (needFilter) {
			sourceInput[0] = filterData(sourceInput[0], insight.subject);
			sourceInput[1] = filterData(sourceInput[1], subject2);
		}
		if (needGroup) {
			sourceInput[0] = groupData(sourceInput[0], insight.subject).get(0);
			sourceInput[1] = groupData(sourceInput[1], subject2).get(0);
		}
		insight.score = computeScore(sourceInput);
		return this.insight;
	}

	/*
		https://www.microsoft.com/en-us/research/uploads/prod/2016/12/Insight-Types-Specification.pdf Significance of Correlation
	 */
	@Override
	public double computeScore(LocalOperator <?>... source) {
		String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap<Object, Double> meaValues1 = initData(source[0].select(columns));
		HashMap<Object, Double> meaValues2 = initData(source[1].select(columns));

		double sum_dot = 0.0;
		double square_sum1 = 0.0;
		double square_sum2 = 0.0;
		double sum1 = 0.0;
		double sum2 = 0.0;
		int count = meaValues1.size();
		if (count <= 2) {
			return 0;
		}
		for (Entry<Object, Double> entry : meaValues1.entrySet()) {
			if (!meaValues2.containsKey(entry.getKey())) {
				continue;
			}
			Double value1 = entry.getValue();
			Double value2 = meaValues2.get(entry.getKey());
			sum_dot += value1 * value2;
			square_sum1 += Math.pow(value1, 2);
			square_sum2 += Math.pow(value2, 2);
			sum1 += value1;
			sum2 += value2;
		}
		double r = (sum_dot - sum1 * sum2 / count) /
			(Math.sqrt(square_sum1 - sum1 * sum1 / count) * Math.sqrt(square_sum2 - sum2 * sum2 / count));
		double t = r * Math.sqrt((count - 1) / (1 - r * r));
		TDistribution tDistribution = new TDistribution(count - 2);
		return tDistribution.cumulativeProbability(Math.abs(t));
	}
}
