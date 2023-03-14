package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.util.List;

public class StepwiseVizData implements AlinkSerializable {
	public String model;
	public String modelSummary;
	public String parameterEsts;
	public String selector;

	public static String formatRows(List <Row> row) {
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < row.size(); ++i) {
			Row data = row.get(i);
			for (int j = 0; j < data.getArity(); j++) {
				Object obj = data.getField(j);
				if (obj instanceof Double || obj instanceof Float) {
					sbd.append(String.format("%.4f", ((Number) obj).doubleValue()));
				} else {
					sbd.append(obj);
				}
				if (j != data.getArity() - 1) {
					sbd.append(",");
				}
			}

			if (i != row.size() - 1) {
				sbd.append("\n");
			}
		}
		return sbd.toString();
	}
}
