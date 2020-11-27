package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.params.evaluation.EvalMultiClassParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.ACCURACY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.ACCURACY_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.CONFUSION_MATRIX;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.F1_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.FALSE_NEGATIVE_RATE_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.FALSE_POSITIVE_RATE_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.KAPPA;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.KAPPA_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_ACCURACY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_F1;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_FALSE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_FALSE_POSITIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_KAPPA;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_PRECISION;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_RECALL;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_SENSITIVITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_SPECIFICITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_TRUE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MACRO_TRUE_POSITIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_ACCURACY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_F1;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_FALSE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_FALSE_POSITIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_KAPPA;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_PRECISION;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_RECALL;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_SENSITIVITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_SPECIFICITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_TRUE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.MICRO_TRUE_POSITIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.PRECISION_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.RECALL_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.SENSITIVITY_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.SPECIFICITY_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.TOTAL_SAMPLES;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.TRUE_NEGATIVE_RATE_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.TRUE_POSITIVE_RATE_ARRAY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_ACCURACY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_F1;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_FALSE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_FALSE_POSITIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_KAPPA;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_PRECISION;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_RECALL;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_SENSITIVITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_SPECIFICITY;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_TRUE_NEGATIVE_RATE;
import static com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics.WEIGHTED_TRUE_POSITIVE_RATE;

/**
 * Common functions for classification evaluation.
 */
public class ClassificationEvaluationUtil implements Serializable {
	static final String STATISTICS_OUTPUT = "Statistics";
	public static final Tuple2 <String, Integer> WINDOW = Tuple2.of("window", 0);
	public static final Tuple2 <String, Integer> ALL = Tuple2.of("all", 1);
	private static final long serialVersionUID = -2732226343798663348L;
	/**
	 * Divide [0,1] into <code>DETAIL_BIN_NUMBER</code> bins.
	 */
	public static int DETAIL_BIN_NUMBER = 100000;
	public static int TOTAL_TRUE = 2;
	public static int TOTAL_FALSE = 3;
	public static int CUR_TRUE = 0;
	public static int CUR_FALSE = 1;
	private static int TPR = 0;
	private static int FPR = 1;
	private static int PRECISION = 2;
	private static int POSITIVE_RATE = 3;
	public static int RECORD_LEN = 4;

	private static double PROBABILITY_ERROR = 0.001;

	/**
	 * Binary Classification label number.
	 */
	public static int BINARY_LABEL_NUMBER = 2;

	/**
	 * return <score, positive/negative, logLoss>
	 *
	 * @param row       Row contains two fields: (label, predicition)
	 * @param tuple     [positiveLabel, negativeLabel]
	 * @param labelType the type of the label
	 * @return <score, positive/negative, logLoss>
	 */
	public static Tuple3 <Double, Boolean, Double> getBinaryDetailStatistics(Row row,
																			 Object[] tuple,
																			 TypeInformation <?> labelType) {
		Preconditions.checkArgument(tuple.length == 2, "Label length is not 2, Only support binary evaluation!");
		if (EvaluationUtil.checkRowFieldNotNull(row)) {
			TreeMap <Object, Double> labelProbMap = EvaluationUtil.extractLabelProbMap(row, labelType);
			Object label = row.getField(0);
			Preconditions.checkState(labelProbMap.size() == BINARY_LABEL_NUMBER,
				"The number of labels must be equal to 2!");
			double logLoss = EvaluationUtil.extractLogloss(labelProbMap, label);

			double d = labelProbMap.get(tuple[0]);
			if (label.equals(tuple[0])) {
				return Tuple3.of(d, true, logLoss);
			} else if (label.equals(tuple[1])) {
				return Tuple3.of(d, false, logLoss);
			}
		}
		return null;
	}

	/**
	 * The evaluation type.
	 */
	public enum Type {
		/**
		 * input columns include predictionDetail and label
		 */
		PRED_DETAIL,
		/**
		 * input columns include prediction and label
		 */
		PRED_RESULT
	}

	/**
	 * Judge the evaluation type from the input.
	 *
	 * @param params If prediction detail column is given, the type is PRED_DETAIL.If prediction detail column is not
	 *               given and prediction column is given, the type is PRED_RESULT.Otherwise, throw exception.
	 * @return the evaluation type.
	 */
	public static Type judgeEvaluationType(Params params) {
		Type type;
		if (params.contains(EvalMultiClassParams.PREDICTION_DETAIL_COL)) {
			type = Type.PRED_DETAIL;
		} else if (params.contains(EvalMultiClassParams.PREDICTION_COL)) {
			type = Type.PRED_RESULT;
		} else {
			throw new IllegalArgumentException("Error Input, must give either predictionCol or predictionDetailCol!");
		}
		return type;
	}

	/**
	 * Give each label an ID, return a map of label and ID.
	 *
	 * @param set           Input set is sorted labels, in multi label case, positiveValue is not supported. The IDs of
	 *                      labels are in descending order of labels.
	 * @param binary        If binary is true, it indicates binary classification, so the the size of labels must be 2.
	 * @param positiveValue It only works when binary is true. In binary label case, if positiveValue not given, it's
	 *                      the same with multi label case. If positiveValue is given, then the id of positiveValue is
	 *                      always 0.
	 * @return a map of label and ID, label array.
	 */
	public static Tuple2 <Map <Object, Integer>, Object[]> buildLabelIndexLabelArray(HashSet <Object> set,
																					 boolean binary,
																					 String positiveValue,
																					 TypeInformation <?> labelType,
																					 boolean classification) {
		Object[] labels = set.toArray();
		Arrays.sort(labels, Collections.reverseOrder());

		Preconditions.checkArgument(!classification || labels.length >= 2, "The distinct label number less than 2!");
		Preconditions.checkArgument(!binary || labels.length == BINARY_LABEL_NUMBER,
			"The number of labels must be equal to 2!");
		Map <Object, Integer> map = new HashMap <>(labels.length);
		if (binary && null != positiveValue) {
			if (EvaluationUtil.labelCompare(labels[1], positiveValue, labelType)) {
				Object tmp = labels[1];
				labels[1] = labels[0];
				labels[0] = tmp;
			} else if (!EvaluationUtil.labelCompare(labels[0], positiveValue, labelType)) {
				throw new IllegalArgumentException("Not contain positiveValue");
			}
			map.put(labels[0], 0);
			map.put(labels[1], 1);
		} else {
			for (int i = 0; i < labels.length; i++) {
				map.put(labels[i], i);
			}
		}
		return Tuple2.of(map, labels);
	}

	/**
	 * Calculate the average value of all labels with frequency as weight.
	 *
	 * @param computer EvaluationMetricComputer
	 * @param matrix   ConfusionMatrix to computer
	 * @return the average
	 */
	private static double frequencyAvgValue(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
											ConfusionMatrix matrix) {
		double total = 0;

		double[] proportion = matrix.getActualLabelProportion();

		for (int i = 0; i < matrix.labelCnt; i++) {
			total += (computer.apply(matrix, i) * proportion[i]);
		}

		return total;
	}

	/**
	 * Calculate the average value of all labels with 1/total as weight.
	 *
	 * @param computer EvaluationMetricComputer
	 * @param matrix   ConfusionMatrix to compute
	 * @return the average
	 */
	private static double macroAvgValue(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
										ConfusionMatrix matrix) {
		double total = 0;
		for (int i = 0; i < matrix.labelCnt; i++) {
			total += computer.apply(matrix, i);
		}
		return total / matrix.labelCnt;
	}

	/**
	 * All the metrics are transformed from TP,TN,FP,FN. First calculate the sum of TP,TN,FP,FN of all labels, and
	 * calculate the metrics using the sum rather than single value.
	 *
	 * @param computer EvaluationMetricComputer
	 * @param matrix   ConfusionMatrix to compute
	 * @return the average
	 */
	private static double microAvgValue(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
										ConfusionMatrix matrix) {
		return computer.apply(matrix, null);
	}

	/**
	 * Return all the values of a given metric.
	 *
	 * @param computer EvaluationMetricComputer
	 * @param matrix   ConfusionMatrix to compute
	 * @return a double array including the metric of all labels and the last three values are frequencyAvg, macroAvg
	 * and microAvg.
	 */
	static double[] getAllValues(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
								 ConfusionMatrix matrix) {
		double[] func = new double[matrix.labelCnt + 3];
		for (int i = 0; i < matrix.labelCnt; i++) {
			func[i] = computer.apply(matrix, i);
		}
		func[matrix.labelCnt] = frequencyAvgValue(computer, matrix);
		func[matrix.labelCnt + 1] = macroAvgValue(computer, matrix);
		func[matrix.labelCnt + 2] = microAvgValue(computer, matrix);
		return func;
	}

	static void setLoglossParams(Params params, double logLoss, long total) {
		if (logLoss >= 0) {
			params.set(BaseSimpleClassifierMetrics.LOG_LOSS, logLoss / total);
		}
	}

	/**
	 * Set the common params for binary and multi classification evaluation.
	 *
	 * @param params          params.
	 * @param confusionMatrix ConfusionMatrix.
	 * @param labels          label array.
	 */
	static void setClassificationCommonParams(Params params, ConfusionMatrix confusionMatrix, String[] labels) {
		params.set(BaseSimpleClassifierMetrics.LABEL_ARRAY, labels);
		params.set(BaseSimpleClassifierMetrics.ACTUAL_LABEL_FREQUENCY, confusionMatrix.getActualLabelFrequency());
		params.set(BaseSimpleClassifierMetrics.ACTUAL_LABEL_PROPORTION, confusionMatrix.getActualLabelProportion());
		params.set(CONFUSION_MATRIX, confusionMatrix.longMatrix.getMatrix());
		params.set(TOTAL_SAMPLES, confusionMatrix.total);

		for (Computations c : Computations.values()) {
			params.set(c.weightedParamInfo, frequencyAvgValue(c.computer, confusionMatrix));
			params.set(c.macroParamInfo, macroAvgValue(c.computer, confusionMatrix));
			params.set(c.microParamInfo, microAvgValue(c.computer, confusionMatrix));
		}
		params.set(ACCURACY, confusionMatrix.getTotalAccuracy());
		params.set(KAPPA, confusionMatrix.getTotalKappa());
	}

	enum Computations {
		/**
		 * True negative, TNR = TN / (FP + TN).
		 */
		TRUE_NEGATIVE(new ClassificationMetricComputers.TrueNegativeRate(), TRUE_NEGATIVE_RATE_ARRAY,
			WEIGHTED_TRUE_NEGATIVE_RATE, MACRO_TRUE_NEGATIVE_RATE, MICRO_TRUE_NEGATIVE_RATE),

		/**
		 * True positive, TPR = TP / (TP + FN).
		 */
		TRUE_POSITIVE(new ClassificationMetricComputers.TruePositiveRate(), TRUE_POSITIVE_RATE_ARRAY,
			WEIGHTED_TRUE_POSITIVE_RATE, MACRO_TRUE_POSITIVE_RATE, MICRO_TRUE_POSITIVE_RATE),

		/**
		 * False negative, FNR = FN / (TP + FN).
		 */
		FALSE_NEGATIVE(new ClassificationMetricComputers.FalseNegativeRate(), FALSE_NEGATIVE_RATE_ARRAY,
			WEIGHTED_FALSE_NEGATIVE_RATE, MACRO_FALSE_NEGATIVE_RATE, MICRO_FALSE_NEGATIVE_RATE),

		/**
		 * False positve, FPR = FP / (FP + TN).
		 */
		FALSE_POSITIVE(new ClassificationMetricComputers.FalsePositiveRate(), FALSE_POSITIVE_RATE_ARRAY,
			WEIGHTED_FALSE_POSITIVE_RATE, MACRO_FALSE_POSITIVE_RATE, MICRO_FALSE_POSITIVE_RATE),

		/**
		 * Precision, precision = TP / (TP + FP).
		 */
		PRECISION(new ClassificationMetricComputers.Precision(), PRECISION_ARRAY, WEIGHTED_PRECISION, MACRO_PRECISION,
			MICRO_PRECISION),

		/**
		 * Specitivity, specitivity = TNR.
		 */
		SPECITIVITY(new ClassificationMetricComputers.TrueNegativeRate(), SPECIFICITY_ARRAY, WEIGHTED_SPECIFICITY,
			MACRO_SPECIFICITY, MICRO_SPECIFICITY),

		/**
		 * Sensitivity, sensitivity = tpr.
		 */
		SENSITIVITY(new ClassificationMetricComputers.TruePositiveRate(), SENSITIVITY_ARRAY, WEIGHTED_SENSITIVITY,
			MACRO_SENSITIVITY, MICRO_SENSITIVITY),

		/**
		 * Recall, recall == tpr.
		 */
		RECALL(new ClassificationMetricComputers.TruePositiveRate(), RECALL_ARRAY, WEIGHTED_RECALL, MACRO_RECALL,
			MICRO_RECALL),

		/**
		 * F1, F1 = 2 * Precision * Recall / (Precision + Recall).
		 */
		F1(new ClassificationMetricComputers.F1(), F1_ARRAY, WEIGHTED_F1, MACRO_F1, MICRO_F1),

		/**
		 * Accuracy, accuracy = (TP + TN) / Total.
		 */
		ACCURACY(new ClassificationMetricComputers.Accuracy(), ACCURACY_ARRAY, WEIGHTED_ACCURACY, MACRO_ACCURACY,
			MICRO_ACCURACY),

		/**
		 * Kappa.
		 */
		KAPPA(new ClassificationMetricComputers.Kappa(), KAPPA_ARRAY, WEIGHTED_KAPPA, MACRO_KAPPA, MICRO_KAPPA);

		ClassificationMetricComputers.BaseClassificationMetricComputer computer;
		ParamInfo <double[]> arrayParamInfo;
		ParamInfo <Double> weightedParamInfo;
		ParamInfo <Double> macroParamInfo;
		ParamInfo <Double> microParamInfo;

		Computations(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
					 ParamInfo <double[]> paramInfo,
					 ParamInfo <Double> weightedParamInfo,
					 ParamInfo <Double> macroParamInfo,
					 ParamInfo <Double> microParamInfo) {
			this.computer = computer;
			this.arrayParamInfo = paramInfo;
			this.weightedParamInfo = weightedParamInfo;
			this.macroParamInfo = macroParamInfo;
			this.microParamInfo = microParamInfo;
		}
	}

	public static class BinaryPartitionSummary implements Serializable {
		private static final long serialVersionUID = 1L;
		Integer taskId;
		double maxScore;
		long curPositive;
		long curNegative;

		public BinaryPartitionSummary(Integer taskId, double maxScore, long curPositive, long curNegative) {
			this.taskId = taskId;
			this.maxScore = maxScore;
			this.curPositive = curPositive;
			this.curNegative = curNegative;
		}
	}

	/**
	 * @param values Summary of different partitions.
	 * @param taskId current taskId.
	 * @return <The first partition, [curTrue, curFalse, TotalTrue, TotalFalse])
	 */
	public static Tuple2 <Boolean, long[]> reduceBinaryPartitionSummary(List <BinaryPartitionSummary> values,
																		int taskId) {
		List <BinaryPartitionSummary> list = new ArrayList <>();
		values.forEach(list::add);
		list.sort(Comparator.comparingDouble(t -> -t.maxScore));
		long curTrue = 0;
		long curFalse = 0;
		long totalTrue = 0;
		long totalFalse = 0;

		boolean firstBin = true;

		for (BinaryPartitionSummary statistics : list) {
			if (statistics.taskId == taskId) {
				firstBin = (totalTrue + totalFalse == 0);
				curFalse = totalFalse;
				curTrue = totalTrue;
			}
			totalTrue += statistics.curPositive;
			totalFalse += statistics.curNegative;
		}
		return Tuple2.of(firstBin, new long[] {curTrue, curFalse, totalTrue, totalFalse});
	}

	public static boolean isMiddlePoint(Tuple3 <Double, Boolean, Double> t) {
		if (Double.compare(t.f0, 0.5) == 0 && t.f1 && Double.isNaN(t.f2)) {
			return true;
		}
		return false;
	}

	public static Tuple3 <Double, Boolean, Double> middlePoint = Tuple3.of(0.5, true, Double.NaN);

	public static void updateBinaryPartitionSummary(BinaryPartitionSummary statistics,
													Tuple3 <Double, Boolean, Double> t) {
		if (!isMiddlePoint(t)) {
			if (t.f1) {
				statistics.curPositive++;
			} else {
				statistics.curNegative++;
			}
		}
		int compare = Double.compare(statistics.maxScore, t.f0);
		if (compare < 0) {
			statistics.maxScore = t.f0;
		}
	}

	public static void updateAccurateBinaryMetricsSummary(Tuple3 <Double, Boolean, Double> cur,
														  AccurateBinaryMetricsSummary binaryMetricsSummary,
														  long[] countValues,
														  double[] recordValues,
														  boolean first) {
		if (binaryMetricsSummary.total == 0) {
			recordValues[TPR] = countValues[TOTAL_TRUE] == 0 ? 1.0
				: 1.0 * countValues[CUR_TRUE] / countValues[TOTAL_TRUE];
			recordValues[FPR] = countValues[TOTAL_FALSE] == 0 ? 1.0
				: 1.0 * countValues[CUR_FALSE] / countValues[TOTAL_FALSE];
			recordValues[PRECISION] = countValues[CUR_TRUE] + countValues[CUR_FALSE] == 0 ? 1.0
				: 1.0 * countValues[CUR_TRUE] / (countValues[CUR_TRUE] + countValues[CUR_FALSE]);
			recordValues[POSITIVE_RATE] = 1.0 * (countValues[CUR_TRUE] + countValues[CUR_FALSE]) / (
				countValues[TOTAL_TRUE] + countValues[TOTAL_FALSE]);
		}

		if (!isMiddlePoint(cur)) {
			binaryMetricsSummary.total++;
			binaryMetricsSummary.logLoss += cur.f2;
			if (cur.f1) {
				countValues[CUR_TRUE]++;
			} else {
				countValues[CUR_FALSE]++;
			}
		}

		double threshold = cur.f0;
		double tpr = countValues[TOTAL_TRUE] == 0 ? 1.0 : 1.0 * countValues[CUR_TRUE] / countValues[TOTAL_TRUE];
		double fpr = countValues[TOTAL_FALSE] == 0 ? 1.0 : 1.0 * countValues[CUR_FALSE] / countValues[TOTAL_FALSE];
		double precision = countValues[CUR_TRUE] + countValues[CUR_FALSE] == 0 ? 1.0
			: 1.0 * countValues[CUR_TRUE] / (countValues[CUR_TRUE] + countValues[CUR_FALSE]);
		double positiveRate = 1.0 * (countValues[CUR_TRUE] + countValues[CUR_FALSE]) / (countValues[TOTAL_TRUE]
			+ countValues[TOTAL_FALSE]);

		if (binaryMetricsSummary.total == 1 && first) {
			recordValues[PRECISION] = precision;
			ConfusionMatrix confusionMatrix = new ConfusionMatrix(
				new long[][] {{0, 0}, {countValues[TOTAL_TRUE], countValues[TOTAL_FALSE]}});
			binaryMetricsSummary.metricsInfoList.add(Tuple2.of(1.0, confusionMatrix));
		}

		//binaryMetricsSummary.auc += ((fpr - recordValues[FPR]) * (tpr + recordValues[TPR]) / 2);
		binaryMetricsSummary.gini += ((positiveRate - recordValues[POSITIVE_RATE]) * (tpr + recordValues[TPR]) / 2);
		binaryMetricsSummary.prc += ((tpr - recordValues[TPR]) * (precision + recordValues[PRECISION]) / 2);
		binaryMetricsSummary.ks = Math.max(Math.abs(fpr - tpr), binaryMetricsSummary.ks);

		recordValues[TPR] = tpr;
		recordValues[FPR] = fpr;
		recordValues[PRECISION] = precision;
		recordValues[POSITIVE_RATE] = positiveRate;

		ConfusionMatrix confusionMatrix = new ConfusionMatrix(
			new long[][] {{countValues[CUR_TRUE], countValues[CUR_FALSE]},
				{countValues[TOTAL_TRUE] - countValues[CUR_TRUE], countValues[TOTAL_FALSE] - countValues[CUR_FALSE]}});

		//keep the middlePoint(p = 0.5), keep the first point(p = 1.0), then compare the threshold
		if (binaryMetricsSummary.metricsInfoList.isEmpty()
			|| (isMiddlePoint(cur) && (binaryMetricsSummary.metricsInfoList.isEmpty()
			|| binaryMetricsSummary.metricsInfoList.get(binaryMetricsSummary.metricsInfoList.size() - 1).f0 != 0.5))
			|| Math.abs(
			threshold - binaryMetricsSummary.metricsInfoList.get(binaryMetricsSummary.metricsInfoList.size() - 1).f0)
			>= PROBABILITY_ERROR) {
			binaryMetricsSummary.metricsInfoList.add(Tuple2.of(threshold, confusionMatrix));
		} else {
			Tuple2 <Double, ConfusionMatrix> metricsInfo = binaryMetricsSummary.metricsInfoList
				.get(binaryMetricsSummary.metricsInfoList.size() - 1);
			metricsInfo.f1 = confusionMatrix;
		}
	}
}
