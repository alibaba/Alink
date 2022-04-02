package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

public class OutlierMetrics extends BaseBinaryClassMetrics <OutlierMetrics> {

	public static final ParamInfo <String[]> OUTLIER_VALUE_ARRAY = ParamInfoFactory
		.createParamInfo("outlierValueArray", String[].class)
		.setDescription("outlier value array")
		.setRequired()
		.build();

	public OutlierMetrics(Row row) {
		super(row);
	}

	public OutlierMetrics(Params params) {
		super(params);
	}

	public String[] getOutlierValueArray() {
		return get(OUTLIER_VALUE_ARRAY);
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("Metrics:", '-'));
		String[] labels = getLabelArray();

		String[][] confusionMatrixStr = new String[2][2];
		long[][] confusionMatrix = getConfusionMatrix();
		confusionMatrixStr[0][0] = String.valueOf(confusionMatrix[0][0]);
		confusionMatrixStr[0][1] = String.valueOf(confusionMatrix[0][1]);
		confusionMatrixStr[1][0] = String.valueOf(confusionMatrix[1][0]);
		confusionMatrixStr[1][1] = String.valueOf(confusionMatrix[1][1]);

		String[] outlierValues = getOutlierValueArray();
		String[] inlierValues = ArrayUtils.removeElements(labels, outlierValues);
		String[] rowNames = new String[] {"Outlier", "Normal"};

		sbd.append("Outlier values: ")
			.append(PrettyDisplayUtils.displayList(Arrays.asList(outlierValues))).append("\t").append("\t")
			.append("Normal values: ")
			.append(PrettyDisplayUtils.displayList(Arrays.asList(inlierValues))).append("\n")
			.append("Auc:").append(PrettyDisplayUtils.display(getAuc())).append("\t")
			.append("Accuracy:").append(PrettyDisplayUtils.display(getAccuracy())).append("\t")
			.append("Precision:").append(PrettyDisplayUtils.display(getPrecision())).append("\t")
			.append("Recall:").append(PrettyDisplayUtils.display(getRecall())).append("\t")
			.append("F1:").append(PrettyDisplayUtils.display(getF1())).append("\n")
			.append(PrettyDisplayUtils.displayTable(confusionMatrixStr, 2, 2, rowNames, rowNames, "Pred\\Real"));
		return sbd.toString();
	}
}
