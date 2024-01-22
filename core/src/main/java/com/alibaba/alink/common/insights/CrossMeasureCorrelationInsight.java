package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

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
		List<Measure> measures = this.insight.subject.measures;
		this.insight.layout.xAxis = measures.get(0).aggr + "(" + measures.get(0).colName + ")";
		this.insight.layout.yAxis = measures.get(1).aggr + "(" + measures.get(1).colName + ")";;
		this.insight.layout.title = String.format("%s的%s", measures.get(0).colName, measures.get(0).aggr.getCnName())
			+ " 和 " + String.format("%s的%s", measures.get(1).colName, measures.get(1).aggr.getCnName()) + " 存在相关性";
		StringBuilder builder = new StringBuilder();
		if (null != insight.subject.subspaces && !insight.subject.subspaces.isEmpty()) {
			builder.append(insight.getSubspaceStr(insight.subject.subspaces)).append(" 条件下，");
		}
		builder.append(String.format("%s的%s", measures.get(0).colName, measures.get(0).aggr.getCnName()))
			.append(" 与 ")
			.append(String.format("%s的%s", measures.get(1).colName, measures.get(1).aggr.getCnName()))
			.append(" 存在相关性");
		this.insight.layout.description = builder.toString();
	}

	public double computeScore(LocalOperator <?>... sources) {
		//String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Number> meaValues1 = initData(sources[0]);
		HashMap <Object, Number> meaValues2 = initData(sources[1]);
		List <Tuple3 <Number, Number, Object>> points = new ArrayList <>();
		for (Entry <Object, Number> entry : meaValues1.entrySet()) {
			if (!meaValues2.containsKey(entry.getKey())) {
				continue;
			}
			points.add(Tuple3.of(entry.getValue(), meaValues2.get(entry.getKey()), entry.getKey()));
		}
		if (points.size() < MIN_SAMPLE_NUM) {
			return 0;
		}
		double[] xArray = new double[points.size()];
		double[] yArray = new double[points.size()];
		double maxY = Double.MIN_VALUE;
		double minY = Double.MAX_VALUE;

		for (int i = 0; i < points.size(); i++) {
			xArray[i] = points.get(i).f0.doubleValue();
			yArray[i] = points.get(i).f1.doubleValue();
			maxY = Math.max(maxY, yArray[i]);
			minY = Math.min(minY, yArray[i]);
		}
		WeightedObservedPoints weightedObservedPoints = new WeightedObservedPoints();
		for (int i = 0; i < points.size(); i++) {
			weightedObservedPoints.add(xArray[i], yArray[i]);
		}
		PolynomialCurveFitter polynomialCurveFitter = PolynomialCurveFitter.create(1);
		double[] params = polynomialCurveFitter.fit(weightedObservedPoints.toList());
		double r2 = 0.0;
		for (int i = 0; i < points.size(); i++) {
			r2 += Math.pow(params[0] + params[1] * xArray[i] - yArray[i], 2);
		}
		double scoreA = 1 - Math.sqrt(r2) / ((maxY - minY) * points.size());
		if (scoreA < 0) {
			return 0;
		}
		//PearsonsCorrelation pc = new PearsonsCorrelation();
		SpearmansCorrelation sc = new SpearmansCorrelation();
		double scoreB = Math.abs(sc.correlation(xArray, yArray));
		double score = (scoreA + scoreB) / 2;
		if (score >= MIN_CORRELATION_THRESHOLD) {
			MTable mtable = mergeData(points, sources[0].getSchema(), sources[1].getSchema());
			insight.layout.data = mtable;
			insight.score = score;
		} else {
			score = 0;
		}
		return score;
	}

}