package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import org.apache.commons.math3.distribution.TDistribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class CrossMeasureCorrelationInsight extends CorrelationInsightBase {

	public CrossMeasureCorrelationInsight(Subject subject, InsightType type) {
		super(subject, type);
	}

	@Override
	public Insight processData(LocalOperator <?>... sources) {
		LocalOperator <?>[] sourceInput = new LocalOperator[2];
		if (sources.length == 1) {
			LocalOperator <?> input = sources[0];
			if (needFilter) {
				input = filterData(input, insight.subject);
			}
			if (needGroup) {
				List<LocalOperator <?>> result = groupData(input, insight.subject);
				sourceInput[0] = result.get(0);
				sourceInput[1] = result.get(1);
			}
		} else {
			sourceInput = sources;
		}
		insight.score = computeScore(sourceInput);
		mergeData(sourceInput);
		return insight;
	}

	public void mergeData(LocalOperator <?>... sources) {
		String schema = TableUtil.schema2SchemaStr(sources[0].getSchema());
		String[] parts1 = TableUtil.schema2SchemaStr(sources[1].getSchema()).split(",");
		String newSchema = schema + "," + MEASURE_NAME_PREFIX + "1 " + parts1[1].trim().split(" ")[1];
		HashMap<Object, Double> measuerValues = initData(sources[1]);
		List<Row> rows = new ArrayList <>();
		for (Row row : sources[0].getOutputTable().getRows()) {
			Object rowkey = row.getField(0);
			if (!measuerValues.containsKey(rowkey)) {
				continue;
			}
			Row newRow = new Row(3);
			newRow.setField(0, row.getField(0));
			newRow.setField(1, row.getField(1));
			newRow.setField(2, measuerValues.get(rowkey));
			rows.add(newRow);
		}
		LayoutData layoutData = new LayoutData();
		layoutData.data = new MTable(rows, newSchema);
		insight.layout = layoutData;
	}

	@Override
	public double computeScore(LocalOperator <?>... source) {
		String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Double> measuerValues0 = initData(source[0].select(columns));
		HashMap<Object, Double> measuerValues1 = initData(source[1].select(columns));

		double sum_dot = 0.0;
		double square_sum1 = 0.0;
		double square_sum2 = 0.0;
		double sum1 = 0.0;
		double sum2 = 0.0;
		int count = measuerValues0.size();
		if (count <= 2) {
			return 0;
		}
		for (Entry <Object, Double> entry : measuerValues0.entrySet()) {
			if (!measuerValues1.containsKey(entry.getKey())) {
				continue;
			}
			Double value1 = entry.getValue();
			Double value2 = measuerValues1.get(entry.getKey());
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