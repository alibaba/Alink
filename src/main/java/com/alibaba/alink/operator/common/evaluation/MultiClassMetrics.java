package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Multi classification evaluation metrics.
 */
public final class MultiClassMetrics extends BaseSimpleClassifierMetrics<MultiClassMetrics> {
	public static final ParamInfo<long[]> PREDICT_LABEL_FREQUENCY = ParamInfoFactory
		.createParamInfo("PredictLabelFrequency", long[].class)
		.setDescription("predict label frequency")
		.setRequired()
		.build();
	public static final ParamInfo<double[]> PREDICT_LABEL_PROPORTION = ParamInfoFactory
		.createParamInfo("PredictLabelProportion", double[].class)
		.setDescription("predict label proportion")
		.setRequired()
		.build();

	public long[] getPredictLabelFrequency() {return get(PREDICT_LABEL_FREQUENCY);}

	public double[] getPredictLabelProportion() {return get(PREDICT_LABEL_PROPORTION);}

	public double getTruePositiveRate(String label) {
		return getParams().get(TRUE_POSITIVE_RATE_ARRAY)[getLabelIndex(label)];
	}

	public double getTrueNegativeRate(String label) {
		return getParams().get(TRUE_NEGATIVE_RATE_ARRAY)[getLabelIndex(label)];
	}

	public double getFalsePositiveRate(String label) {
		return getParams().get(FALSE_POSITIVE_RATE_ARRAY)[getLabelIndex(label)];
	}

	public double getFalseNegativeRate(String label) {
		return getParams().get(FALSE_NEGATIVE_RATE_ARRAY)[getLabelIndex(label)];
	}

	public double getPrecision(String label) {
		return getParams().get(PRECISION_ARRAY)[getLabelIndex(label)];
	}

	public double getSpecificity(String label) {
		return getParams().get(SPECIFICITY_ARRAY)[getLabelIndex(label)];
	}

	public double getSensitivity(String label) {
		return getParams().get(SENSITIVITY_ARRAY)[getLabelIndex(label)];
	}

	public double getRecall(String label) {
		return getParams().get(RECALL_ARRAY)[getLabelIndex(label)];
	}

	public double getF1(String label) {
		return getParams().get(F1_ARRAY)[getLabelIndex(label)];
	}

	public double getAccuracy(String label) {
		return getParams().get(ACCURACY_ARRAY)[getLabelIndex(label)];
	}

	public double getKappa(String label) {
		return getParams().get(KAPPA_ARRAY)[getLabelIndex(label)];
	}

	private int getLabelIndex(String label){
		int index = ArrayUtils.indexOf(getParams().get(LABEL_ARRAY), label);
		Preconditions.checkArgument(index >= 0, String.format("Not exist label %s", label));
		return index;
	}

	public MultiClassMetrics(Row row) {
		super(row);
	}

	public MultiClassMetrics(Params params) {
		super(params);
	}
}
