package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * MultiLabel classification evaluation metrics.
 */
public abstract class BaseSimpleMultiLabelMetrics<T extends BaseSimpleMultiLabelMetrics <T>> extends BaseMetrics <T> {
	private static final long serialVersionUID = -2839172794112883835L;

	static final ParamInfo <String[]> LABEL_ARRAY = ParamInfoFactory
		.createParamInfo("labelArray", String[].class)
		.setDescription("label array")
		.setRequired()
		.build();

	static final ParamInfo <double[]> PRECISION_ARRAY = ParamInfoFactory
		.createParamInfo("precisionArray", double[].class)
		.setDescription("precision list, PRECISION: TP / (TP + FP)")
		.setRequired()
		.build();

	static final ParamInfo <double[]> RECALL_ARRAY = ParamInfoFactory
		.createParamInfo("recallArray", double[].class)
		.setDescription("recall list, recall == TPR")
		.setRequired()
		.build();

	static final ParamInfo <double[]> F1_ARRAY = ParamInfoFactory
		.createParamInfo("f1Array", double[].class)
		.setDescription("F1 list, F1: 2 * TP / (2TP + FP + FN)")
		.setRequired()
		.build();

	static final ParamInfo <Double> SUBSET_ACCURACY = ParamInfoFactory
		.createParamInfo("subsetAccuracy", Double.class)
		.setDescription("SubsetAccuracy ")
		.setRequired()
		.build();

	static final ParamInfo <Double> HAMMING_LOSS = ParamInfoFactory
		.createParamInfo("hammingLoss", Double.class)
		.setDescription("HammingLoss, ")
		.setRequired()
		.build();

	static final ParamInfo <Double> ACCURACY = ParamInfoFactory
		.createParamInfo("accuracy", Double.class)
		.setDescription("accuracy, ")
		.setRequired()
		.build();

	static final ParamInfo <Double> MICRO_PRECISION = ParamInfoFactory
		.createParamInfo("microPrecision", Double.class)
		.setDescription("micro precision")
		.setRequired()
		.build();

	static final ParamInfo <Double> MICRO_RECALL = ParamInfoFactory
		.createParamInfo("microRecall", Double.class)
		.setDescription("micro recall")
		.setRequired()
		.build();

	static final ParamInfo <Double> MICRO_F1 = ParamInfoFactory
		.createParamInfo("microF1", Double.class)
		.setDescription("micro f1")
		.setRequired()
		.build();

	static final ParamInfo <Double> PRECISION = ParamInfoFactory
		.createParamInfo("precision", Double.class)
		.setDescription("precision")
		.setRequired()
		.build();

	static final ParamInfo <Double> RECALL = ParamInfoFactory
		.createParamInfo("recall", Double.class)
		.setDescription("recall")
		.setRequired()
		.build();

	static final ParamInfo <Double> F1 = ParamInfoFactory
		.createParamInfo("f1", Double.class)
		.setDescription("f1")
		.setRequired()
		.build();

	public BaseSimpleMultiLabelMetrics(Row row) {
		super(row);
	}

	public BaseSimpleMultiLabelMetrics(Params params) {
		super(params);
	}

	public double getPrecision() {
		return get(PRECISION);
	}

	public double getRecall() {
		return get(RECALL);
	}

	public double getF1() {
		return get(F1);
	}

	public double getAccuracy() {
		return get(ACCURACY);
	}

	public double getHammingLoss() {
		return get(HAMMING_LOSS);
	}

	public double getMicroPrecision() {
		return get(MICRO_PRECISION);
	}

	public double getMicroRecall() {
		return get(MICRO_RECALL);
	}

	public double getMicroF1() {
		return get(MICRO_F1);
	}

	public double getSubsetAccuracy() {
		return get(SUBSET_ACCURACY);
	}
}
