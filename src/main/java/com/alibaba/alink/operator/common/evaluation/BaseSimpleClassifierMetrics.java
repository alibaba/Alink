package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Common evaluation metrics for binary classification and multi classification.
 */
public abstract class BaseSimpleClassifierMetrics<T extends BaseSimpleClassifierMetrics<T>> extends BaseMetrics<T> {
	public static final ParamInfo <long[][]> CONFUSION_MATRIX = ParamInfoFactory
		.createParamInfo("ConfusionMatrix", long[][].class)
		.setDescription("confusionMatrix, [TP FP][FN TN]")
		.setRequired()
		.build();
	public static final ParamInfo <String[]> LABEL_ARRAY = ParamInfoFactory
		.createParamInfo("LabelArray", String[].class)
		.setDescription("label array")
		.setRequired()
		.build();
	public static final ParamInfo <Double> LOG_LOSS = ParamInfoFactory
		.createParamInfo("LogLoss", Double.class)
		.setDescription("logloss, -sum(y_i*log(p_i) + (1-y_i)*log(1-p_i))")
		.setHasDefaultValue(null)
		.build();
	public static final ParamInfo <Long> TOTAL_SAMPLES = ParamInfoFactory
		.createParamInfo("TotalSamples", Long.class)
		.setDescription("total samples")
		.setRequired()
		.build();
	public static final ParamInfo <long[]> ACTUAL_LABEL_FREQUENCY = ParamInfoFactory
		.createParamInfo("ActualLabelFrequency", long[].class)
		.setDescription("actual label frequency")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> ACTUAL_LABEL_PROPORTION = ParamInfoFactory
		.createParamInfo("ActualLabelProportion", double[].class)
		.setDescription("actual label proportion")
		.setRequired()
		.build();
	public static final ParamInfo <Double> ACCURACY = ParamInfoFactory
		.createParamInfo("Accuracy", Double.class)
		.setDescription("accuracy, ")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_ACCURACY = ParamInfoFactory
		.createParamInfo("MacroAccuracy", Double.class)
		.setDescription("macro accuracy")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_ACCURACY = ParamInfoFactory
		.createParamInfo("MicroAccuracy", Double.class)
		.setDescription("micro accuracy")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_ACCURACY = ParamInfoFactory
		.createParamInfo("WeightedAccuracy", Double.class)
		.setDescription("weighted accuracy")
		.setRequired()
		.build();
	public static final ParamInfo <Double> KAPPA = ParamInfoFactory
		.createParamInfo("Kappa", Double.class)
		.setDescription("kappa")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_KAPPA = ParamInfoFactory
		.createParamInfo("MacroKappa", Double.class)
		.setDescription("macro kappa")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_KAPPA = ParamInfoFactory
		.createParamInfo("MicroKappa", Double.class)
		.setDescription("micro kappa")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_KAPPA = ParamInfoFactory
		.createParamInfo("WeightedKappa", Double.class)
		.setDescription("weighted kappa")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_PRECISION = ParamInfoFactory
		.createParamInfo("MacroPrecision", Double.class)
		.setDescription("macro precision")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_PRECISION = ParamInfoFactory
		.createParamInfo("MicroPrecision", Double.class)
		.setDescription("micro precision")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_PRECISION = ParamInfoFactory
		.createParamInfo("WeightedPrecision", Double.class)
		.setDescription("weighted precision")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_RECALL = ParamInfoFactory
		.createParamInfo("MacroRecall", Double.class)
		.setDescription("macro recall")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_RECALL = ParamInfoFactory
		.createParamInfo("MicroRecall", Double.class)
		.setDescription("micro recall")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_RECALL = ParamInfoFactory
		.createParamInfo("WeightedRecall", Double.class)
		.setDescription("weighted recall")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_F1 = ParamInfoFactory
		.createParamInfo("MacroF1", Double.class)
		.setDescription("macro f1")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_F1 = ParamInfoFactory
		.createParamInfo("MicroF1", Double.class)
		.setDescription("micro f1")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_F1 = ParamInfoFactory
		.createParamInfo("WeightedF1", Double.class)
		.setDescription("weighted f1")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_SENSITIVITY = ParamInfoFactory
		.createParamInfo("MacroSensitivity", Double.class)
		.setDescription("macro sensitivity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_SENSITIVITY = ParamInfoFactory
		.createParamInfo("MicroSensitivity", Double.class)
		.setDescription("micro sensitivity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_SENSITIVITY = ParamInfoFactory
		.createParamInfo("WeightedSensitivity", Double.class)
		.setDescription("weighted sensitivity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_SPECIFICITY = ParamInfoFactory
		.createParamInfo("MacroSpecificity", Double.class)
		.setDescription("macro specificity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_SPECIFICITY = ParamInfoFactory
		.createParamInfo("MicroSpecificity", Double.class)
		.setDescription("micro specificity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_SPECIFICITY = ParamInfoFactory
		.createParamInfo("WeightedSpecificity", Double.class)
		.setDescription("weighted specificity")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_TRUE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("MacroTruePositiveRate", Double.class)
		.setDescription("macro true positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_TRUE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("MicroTruePositiveRate", Double.class)
		.setDescription("micro true positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_TRUE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("WeightedTruePositiveRate", Double.class)
		.setDescription("weighted true positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_TRUE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("MacroTrueNegativeRate", Double.class)
		.setDescription("macro true negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_TRUE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("MicroTrueNegativeRate", Double.class)
		.setDescription("micro true negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_TRUE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("WeightedTrueNegativeRate", Double.class)
		.setDescription("weighted true negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_FALSE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("MacroFalsePositiveRate", Double.class)
		.setDescription("macro false positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_FALSE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("MicroFalsePositiveRate", Double.class)
		.setDescription("micro false positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_FALSE_POSITIVE_RATE = ParamInfoFactory
		.createParamInfo("WeightedFalsePositiveRate", Double.class)
		.setDescription("weighted false positive rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MACRO_FALSE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("MacroFalseNegativeRate", Double.class)
		.setDescription("macro false negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> MICRO_FALSE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("MicroFalseNegativeRate", Double.class)
		.setDescription("micro false negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <Double> WEIGHTED_FALSE_NEGATIVE_RATE = ParamInfoFactory
		.createParamInfo("WeightedFalseNegativeRate", Double.class)
		.setDescription("weighted false negative rate")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> TRUE_POSITIVE_RATE_ARRAY = ParamInfoFactory
		.createParamInfo("TruePositiveRateArray", double[].class)
		.setDescription("true positive rate, TPR = TP / (TP + FN)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> TRUE_NEGATIVE_RATE_ARRAY = ParamInfoFactory
		.createParamInfo("TrueNegativeRateArray", double[].class)
		.setDescription("true negative rate, TNR = TN / (FP + TN)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> FALSE_POSITIVE_RATE_ARRAY = ParamInfoFactory
		.createParamInfo("FalsePositiveRateArray", double[].class)
		.setDescription("false positive rate, FPR = FP / (FP + TN)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> FALSE_NEGATIVE_RATE_ARRAY = ParamInfoFactory
		.createParamInfo("FalseNegativeRateArray", double[].class)
		.setDescription("false negative rate, FNR = FN / (TP + FN)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> PRECISION_ARRAY = ParamInfoFactory
		.createParamInfo("PrecisionArray", double[].class)
		.setDescription("precision list, PRECISION: TP / (TP + FP)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> SPECIFICITY_ARRAY = ParamInfoFactory
		.createParamInfo("SpecificityArray", double[].class)
		.setDescription("Specificity list, Specificity = TNR")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> SENSITIVITY_ARRAY = ParamInfoFactory
		.createParamInfo("SensitivityArray", double[].class)
		.setDescription("sensitivity list, sensitivity = TPR")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> RECALL_ARRAY = ParamInfoFactory
		.createParamInfo("RecallArray", double[].class)
		.setDescription("recall list, recall == TPR")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> F1_ARRAY = ParamInfoFactory
		.createParamInfo("F1Array", double[].class)
		.setDescription("F1 list, F1: 2 * TP / (2TP + FP + FN)")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> ACCURACY_ARRAY = ParamInfoFactory
		.createParamInfo("AccuracyArray", double[].class)
		.setDescription("accuracy list, ACCURACY: (TP + TN) / Total")
		.setRequired()
		.build();
	public static final ParamInfo <double[]> KAPPA_ARRAY = ParamInfoFactory
		.createParamInfo("KappaArray", double[].class)
		.setDescription("kappa list")
		.setRequired()
		.build();

	public long[][] getConfusionMatrix() {return get(CONFUSION_MATRIX);}

	public String[] getLabelArray() {return get(LABEL_ARRAY);}

	public Double getLogLoss() {return get(LOG_LOSS);}

	public Long getTotalSamples() {return get(TOTAL_SAMPLES);}

	public long[] getActualLabelFrequency() {return get(ACTUAL_LABEL_FREQUENCY);}

	public double[] getActualLabelProportion() {return get(ACTUAL_LABEL_PROPORTION);}

	public double getAccuracy() {
		return get(ACCURACY);
	}

	public double getMacroAccuracy() {
		return get(MACRO_ACCURACY);
	}

	public double getMicroAccuracy() {
		return get(MICRO_ACCURACY);
	}

	public double getWeightedAccuracy() {
		return get(WEIGHTED_ACCURACY);
	}

	public double getKappa() {
		return get(KAPPA);
	}

	public double getMacroKappa() {
		return get(MACRO_KAPPA);
	}

	public double getMicroKappa() {
		return get(MICRO_KAPPA);
	}

	public double getWeightedKappa() {
		return get(WEIGHTED_KAPPA);
	}

	public double getMacroPrecision() {
		return get(MACRO_PRECISION);
	}

	public double getMicroPrecision() {
		return get(MICRO_PRECISION);
	}

	public double getWeightedPrecision() {
		return get(WEIGHTED_PRECISION);
	}

	public double getMacroRecall() {
		return get(MACRO_RECALL);
	}

	public double getMicroRecall() {
		return get(MICRO_RECALL);
	}

	public double getWeightedRecall() {
		return get(WEIGHTED_RECALL);
	}

	public double getMacroF1() {
		return get(MACRO_F1);
	}

	public double getMicroF1() {
		return get(MICRO_F1);
	}

	public double getWeightedF1() {
		return get(WEIGHTED_F1);
	}

	public double getMacroSensitivity() {
		return get(MACRO_SENSITIVITY);
	}

	public double getMicroSensitivity() {
		return get(MICRO_SENSITIVITY);
	}

	public double getWeightedSensitivity() {
		return get(WEIGHTED_SENSITIVITY);
	}

	public double getMacroSpecificity() {
		return get(MACRO_SPECIFICITY);
	}

	public double getMicroSpecificity() {
		return get(MICRO_SPECIFICITY);
	}

	public double getWeightedSpecificity() {
		return get(WEIGHTED_SPECIFICITY);
	}

	public double getMacroTruePositiveRate() {
		return get(MACRO_TRUE_POSITIVE_RATE);
	}

	public double getMicroTruePositiveRate() {
		return get(MICRO_TRUE_POSITIVE_RATE);
	}

	public double getWeightedTruePositiveRate() {
		return get(WEIGHTED_TRUE_POSITIVE_RATE);
	}

	public double getMacroTrueNegativeRate() {
		return get(MACRO_TRUE_NEGATIVE_RATE);
	}

	public double getMicroTrueNegativeRate() {
		return get(MICRO_TRUE_NEGATIVE_RATE);
	}

	public double getWeightedTrueNegativeRate() {
		return get(WEIGHTED_TRUE_NEGATIVE_RATE);
	}

	public double getMacroFalsePositiveRate() {
		return get(MACRO_FALSE_POSITIVE_RATE);
	}

	public double getMicroFalsePositiveRate() {
		return get(MICRO_FALSE_POSITIVE_RATE);
	}

	public double getWeightedFalsePositiveRate() {
		return get(WEIGHTED_FALSE_POSITIVE_RATE);
	}

	public double getMacroFalseNegativeRate() {
		return get(MACRO_FALSE_NEGATIVE_RATE);
	}

	public double getMicroFalseNegativeRate() {
		return get(MICRO_FALSE_NEGATIVE_RATE);
	}

	public double getWeightedFalseNegativeRate() {
		return get(WEIGHTED_FALSE_NEGATIVE_RATE);
	}

	public BaseSimpleClassifierMetrics(Row row) {
		super(row);
	}

	public BaseSimpleClassifierMetrics(Params params){
		super(params);
	}

}
