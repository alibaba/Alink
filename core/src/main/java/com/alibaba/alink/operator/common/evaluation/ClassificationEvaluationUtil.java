package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.MTableUtil.GroupFunction;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp.LABELS;
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
	public static final String STATISTICS_OUTPUT = "Statistics";
	public static final Tuple2 <String, Integer> WINDOW = Tuple2.of("window", 0);
	public static final Tuple2 <String, Integer> ALL = Tuple2.of("all", 1);

	private static final long serialVersionUID = -2732226343798663348L;

	private static final Logger LOG = LoggerFactory.getLogger(ClassificationEvaluationUtil.class);

	public static String LABELS_BC_NAME = "labels";
	public static String DECISION_THRESHOLD_BC_NAME = "score_boundary";
	public static String PARTITION_SUMMARIES_BC_NAME = "partition_summaries";
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
		return getBinaryDetailStatistics(row, tuple, labelType, new DefaultLabelProbMapExtractor());
	}

	public static Tuple3 <Double, Boolean, Double> getBinaryDetailStatistics(Row row,
																			 Object[] tuple,
																			 TypeInformation <?> labelType,
																			 LabelProbMapExtractor extractor) {
		AkPreconditions.checkArgument(tuple.length == 2, "Label length is not 2, Only support binary evaluation!");
		if (EvaluationUtil.checkRowFieldNotNull(row)) {
			TreeMap <Object, Double> labelProbMap = EvaluationUtil.extractLabelProbMap(row, labelType, extractor);
			Object label = row.getField(0);
			AkPreconditions.checkState(labelProbMap.size() == BINARY_LABEL_NUMBER,
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
	 * For every partition, calculate (partial) evaluation metrics.
	 *
	 * @param labels            contains 1 entry, i.e. label info (label-index map and label array)
	 * @param sampleStatistics  simple statistics for every sample, including: score to be positive, is real be
	 *                          positive, and log loss
	 * @param decisionThreshold contains 1 entry, i.e. the decision threshold of score (For binary classification, it's
	 *                          just 0.5)
	 * @return a `DataSet` of `AccurateBinaryMetricsSummary`
	 */
	public static DataSet <BaseMetricsSummary> calLabelPredDetailLocal(
		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels,
		DataSet <Tuple3 <Double, Boolean, Double>> sampleStatistics,
		DataSet <Double> decisionThreshold) {
		/*
		  Data is partitioned by range and sort on prediction probability to be positive.
		 */
		sampleStatistics = sampleStatistics.partitionByRange(0)
			.sortPartition(0, Order.DESCENDING);

		/*
		  For each partition, calculate `BinaryPartitionSummary` from sample statistics, including #positive samples,
		  #negative samples, and maximum prediction probability to be positive.
		 */
		DataSet <BinaryPartitionSummary> partitionSummaries = sampleStatistics
			.mapPartition(new CalcBinaryPartitionSummary())
			.withBroadcastSet(decisionThreshold, DECISION_THRESHOLD_BC_NAME);

		// Calculate AUC from `BinaryPartitionSummary`
		DataSet <Tuple1 <Double>> auc = sampleStatistics
			.mapPartition(new CalcSampleOrders())
			.withBroadcastSet(partitionSummaries, PARTITION_SUMMARIES_BC_NAME)
			.withBroadcastSet(decisionThreshold, DECISION_THRESHOLD_BC_NAME)
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Double, Long, Boolean>, Tuple1 <Double>>() {
				private static final long serialVersionUID = -7442946470184046220L;

				@Override
				public void reduce(Iterable <Tuple3 <Double, Long, Boolean>> values, Collector <Tuple1 <Double>> out) {
					long sum = 0;
					long cnt = 0;
					long positiveCnt = 0;
					for (Tuple3 <Double, Long, Boolean> t : values) {
						sum += t.f1;
						cnt++;
						if (t.f2) {
							positiveCnt++;
						}
					}
					out.collect(Tuple1.of(1. * sum / cnt * positiveCnt));
				}
			}).sum(0);

		return sampleStatistics.mapPartition(new CalcBinaryMetricsSummary())
			.withBroadcastSet(partitionSummaries, PARTITION_SUMMARIES_BC_NAME)
			.withBroadcastSet(labels, LABELS)
			.withBroadcastSet(auc, "auc")
			.withBroadcastSet(decisionThreshold, DECISION_THRESHOLD_BC_NAME);
	}

	/**
	 * Calculate {@link BinaryPartitionSummary} for each partition from a dataset of tuples (score, is real positive,
	 * logloss).
	 * <p>
	 * {@link #decisionThreshold} is used to exclude the special entry from counting positive/negative samples.
	 * <p>
	 * Note that scores are not necessary to be in range [0, 1] or to be positive, so initial value of `maxScore` is
	 * `-Double.MAX_VALUE`.
	 */
	static class CalcBinaryPartitionSummary
		extends RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, BinaryPartitionSummary> {

		private double decisionThreshold = 0.5;
		private static final long serialVersionUID = 9012670438603117070L;

		@Override
		public void open(Configuration parameters) {
			decisionThreshold = getRuntimeContext().hasBroadcastVariable(DECISION_THRESHOLD_BC_NAME)
				? getRuntimeContext(). <Double>getBroadcastVariable(DECISION_THRESHOLD_BC_NAME).get(0)
				: 0.5;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> values,
								 Collector <BinaryPartitionSummary> out) {
			BinaryPartitionSummary statistics = new BinaryPartitionSummary(
				getRuntimeContext().getIndexOfThisSubtask(), -Double.MAX_VALUE, 0, 0);
			values.forEach(t -> updateBinaryPartitionSummary(statistics, t, decisionThreshold));
			out.collect(statistics);
		}
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
	public static Tuple2 <Map <Object, Integer>, Object[]> buildLabelIndexLabelArray(Set <Object> set,
																					 boolean binary,
																					 String positiveValue,
																					 TypeInformation <?> labelType,
																					 boolean classification) {
		Object[] labels = set.toArray();
		Arrays.sort(labels, Collections.reverseOrder());

		// For multi-classification evaluation, #labels == 1 gives reasonable results.
		// So, it is OK to only check #labels in binary classification evaluation.
		AkPreconditions.checkArgument(!binary || labels.length == BINARY_LABEL_NUMBER,
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
			arrayParamInfo = paramInfo;
			this.weightedParamInfo = weightedParamInfo;
			this.macroParamInfo = macroParamInfo;
			this.microParamInfo = microParamInfo;
		}
	}

	public static class BinaryPartitionSummary implements Serializable {
		private static final long serialVersionUID = 1L;
		Integer taskId;
		// maximum score in this partition
		double maxScore;
		// #real positives in this partition
		long curPositive;
		// #real negatives in this partition
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
		List <BinaryPartitionSummary> list = new ArrayList <>(values);
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

	public static boolean isMiddlePoint(Tuple3 <Double, Boolean, Double> t, double middleThreshold) {
		return Double.compare(t.f0, middleThreshold) == 0 && t.f1 && Double.isNaN(t.f2);
	}

	public static void updateBinaryPartitionSummary(BinaryPartitionSummary statistics,
													Tuple3 <Double, Boolean, Double> t,
													double middleThreshold) {
		if (!isMiddlePoint(t, middleThreshold)) {
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
														  boolean first,
														  double decisionThreshold, double largestThreshold) {
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

		if (!isMiddlePoint(cur, decisionThreshold)) {
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

		List <Tuple2 <Double, ConfusionMatrix>> metricsInfoList = binaryMetricsSummary.metricsInfoList;
		if (binaryMetricsSummary.total == 1 && first) {
			recordValues[PRECISION] = precision;
			ConfusionMatrix confusionMatrix = new ConfusionMatrix(
				new long[][] {{0, 0}, {countValues[TOTAL_TRUE], countValues[TOTAL_FALSE]}});
			metricsInfoList.add(Tuple2.of(largestThreshold, confusionMatrix));
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

		//keep the middlePoint(p = decisionThreshold), keep the first point(p = 1.0), then compare the threshold
		if (metricsInfoList.isEmpty()
			|| (isMiddlePoint(cur, decisionThreshold) && (metricsInfoList.isEmpty()
			|| metricsInfoList.get(metricsInfoList.size() - 1).f0 != decisionThreshold))
			|| Math.abs(threshold - metricsInfoList.get(metricsInfoList.size() - 1).f0) >= PROBABILITY_ERROR) {
			metricsInfoList.add(Tuple2.of(threshold, confusionMatrix));
		} else {
			Tuple2 <Double, ConfusionMatrix> metricsInfo = metricsInfoList.get(metricsInfoList.size() - 1);
			metricsInfo.f1 = confusionMatrix;
		}
	}

	public static List <Tuple3 <Double, Boolean, Double>> calcSampleStatistics(
		List <Row> data, Tuple2 <Map <Object, Integer>, Object[]> labels, TypeInformation <?> labelType) {
		return calcSampleStatistics(data, labels, labelType, 0.5, new DefaultLabelProbMapExtractor());
	}

	public static List <Tuple3 <Double, Boolean, Double>> calcSampleStatistics(
		List <Row> data, Tuple2 <Map <Object, Integer>, Object[]> labels, TypeInformation <?> labelType,
		Double decisionThreshold, LabelProbMapExtractor extractor) {
		ArrayList <Tuple3 <Double, Boolean, Double>> list = new ArrayList <>();
		for (Row row : data) {
			Tuple3 <Double, Boolean, Double> t = getBinaryDetailStatistics(row, labels.f1, labelType, extractor);
			if (null != t) {
				list.add(t);
			}
		}
		list.add(Tuple3.of(decisionThreshold, true, Double.NaN));
		return list;
	}

	public static AccurateBinaryMetricsSummary calLabelPredDetailLocal(Tuple2 <Map <Object, Integer>, Object[]> labels,
																	   List <Tuple3 <Double, Boolean, Double>> values,
																	   Double decisionThreshold) {
		BinaryPartitionSummary statistics = new BinaryPartitionSummary(0, -Double.MAX_VALUE, 0, 0);
		values.forEach(t -> updateBinaryPartitionSummary(statistics, t, decisionThreshold));

		values.sort(new Comparator <Tuple3 <Double, Boolean, Double>>() {
			@Override
			public int compare(Tuple3 <Double, Boolean, Double> o1, Tuple3 <Double, Boolean, Double> o2) {
				return -o1.f0.compareTo(o2.f0);
			}
		});

		ArrayList <BinaryPartitionSummary> binaryPartitionSummaries = new ArrayList <>();
		binaryPartitionSummaries.add(statistics);
		Tuple2 <Boolean, long[]> t = reduceBinaryPartitionSummary(binaryPartitionSummaries, 0);
		long startIndex = t.f1[CUR_FALSE] + t.f1[CUR_TRUE] + 1;
		long total = t.f1[TOTAL_TRUE] + t.f1[TOTAL_FALSE];

		//Double, Long, Boolean
		List <Row> out = new ArrayList <>();
		for (Tuple3 <Double, Boolean, Double> t3 : values) {
			if (!isMiddlePoint(t3, decisionThreshold)) {
				out.add(Row.of(t3.f0, total - startIndex + 1, t3.f1));
				startIndex++;
			}
		}

		MTable mt = new MTable(out, "f0 double, f1 long, f2 boolean");

		out = MTableUtil.groupFunc(mt, new String[] {"f0"}, new GroupFunction() {
			@Override
			public void calc(List <Row> values, Collector <Row> out) {
				long sum = 0;
				long cnt = 0;
				long positiveCnt = 0;
				for (Row row : values) {
					sum += (Long) row.getField(1);
					cnt++;
					if ((Boolean) row.getField(2)) {
						positiveCnt++;
					}
				}
				out.collect(Row.of(1. * sum / cnt * positiveCnt));
			}
		});

		double auc = 0.0;
		for (Row row : out) {
			auc += (Double) row.getField(0);
		}


		boolean firstBin = t.f0;
		long[] countValues = t.f1;
		long totalTrue = countValues[TOTAL_TRUE];
		long totalFalse = countValues[TOTAL_FALSE];
		if (totalTrue == 0) {
			LOG.warn("There is no positive sample in data!");
		}
		if (totalFalse == 0) {
			LOG.warn("There is no negative sample in data!");
		}
		if (totalTrue > 0 && totalFalse > 0) {
			auc = (auc - 1. * totalTrue * (totalTrue + 1) / 2) / (totalTrue * totalFalse);
		} else {
			auc = Double.NaN;
		}
		double largestThreshold = Double.POSITIVE_INFINITY;

		AccurateBinaryMetricsSummary summary =
			new AccurateBinaryMetricsSummary(labels.f1, decisionThreshold, 0.0, 0L, auc);
		double[] tprFprPrecision = new double[RECORD_LEN];
		for (Tuple3 <Double, Boolean, Double> t3 : values) {
			updateAccurateBinaryMetricsSummary(
				t3,
				summary,
				countValues,
				tprFprPrecision,
				firstBin,
				decisionThreshold,
				largestThreshold);
		}

		return summary;

	}

	/**
	 * For every sample, convert the label-score map to simple statistics, including: score to be positive, is real
	 * positive, and log loss.
	 *
	 * @param data              data samples where 1st field real label and 2nd field is the prediction details
	 * @param labels            contains 1 entry: label-index map and label array
	 * @param labelType         label type
	 * @param decisionThreshold decision threshold of score (for binary classification, it is 0.5)
	 * @param extractor         extract label-score map from prediction details
	 * @return statistics for every sample, including: score to be positive, is real positive, and log loss
	 */
	public static DataSet <Tuple3 <Double, Boolean, Double>> calcSampleStatistics(
		DataSet <Row> data, DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels,
		TypeInformation <?> labelType, DataSet <Double> decisionThreshold, LabelProbMapExtractor extractor) {
		return data.rebalance()
			.mapPartition(new SampleStatisticsMapPartitionFunction(labelType, extractor))
			.withBroadcastSet(labels, LABELS_BC_NAME)
			.withBroadcastSet(decisionThreshold, DECISION_THRESHOLD_BC_NAME);
	}

	/**
	 * This overloaded version is for binary classification usage, where default extractor is used and decision
	 * threshold is set to 0.5.
	 */
	public static DataSet <Tuple3 <Double, Boolean, Double>> calcSampleStatistics(
		DataSet <Row> data, DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels,
		TypeInformation <?> labelType) {
		return calcSampleStatistics(data, labels, labelType,
			labels.getExecutionEnvironment().fromElements(0.5),
			new DefaultLabelProbMapExtractor());
	}

	/**
	 * For each sample, calculate simple statistics.
	 * <p>
	 * Input row contains two fields: real label and prediction details.
	 * <p>
	 * Output is a tuple of (positive score, is real positive, log loss)
	 */
	static class SampleStatisticsMapPartitionFunction
		extends RichMapPartitionFunction <Row, Tuple3 <Double, Boolean, Double>> {
		private static final long serialVersionUID = 5680342197308160013L;

		private Tuple2 <Map <Object, Integer>, Object[]> map;
		private final TypeInformation <?> labelType;
		private final LabelProbMapExtractor extractor;
		private double decisionThreshold = 0.5;

		public SampleStatisticsMapPartitionFunction(TypeInformation <?> labelType, LabelProbMapExtractor extractor) {
			this.labelType = labelType;
			this.extractor = extractor;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			AkPreconditions.checkState(list.size() > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
			map = list.get(0);

			decisionThreshold = getRuntimeContext().hasBroadcastVariable(DECISION_THRESHOLD_BC_NAME)
				? getRuntimeContext(). <Double>getBroadcastVariable(DECISION_THRESHOLD_BC_NAME).get(0)
				: 0.5;
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <Tuple3 <Double, Boolean, Double>> collector) {
			for (Row row : rows) {
				Tuple3 <Double, Boolean, Double> t = getBinaryDetailStatistics(row, map.f1, labelType, extractor);
				if (null != t) {
					collector.collect(t);
				}
			}
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				collector.collect(Tuple3.of(decisionThreshold, true, Double.NaN));
			}
		}
	}

	/**
	 * For each sample, calculate its score order among all samples. The sample with minimum score has order 1, while
	 * the sample with maximum score has order #samples.
	 * <p>
	 * Input is a dataset of tuple (score, is real positive, logloss), output is a dataset of tuple (score, order, is
	 * real positive).
	 */
	static class CalcSampleOrders
		extends RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, Tuple3 <Double, Long, Boolean>> {
		private static final long serialVersionUID = 3047511137846831576L;
		private long startIndex;
		private long total;
		private double decisionThreshold;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <BinaryPartitionSummary> statistics = getRuntimeContext()
				.getBroadcastVariable(PARTITION_SUMMARIES_BC_NAME);
			Tuple2 <Boolean, long[]> t = reduceBinaryPartitionSummary(statistics,
				getRuntimeContext().getIndexOfThisSubtask());
			startIndex = t.f1[CUR_FALSE] + t.f1[CUR_TRUE] + 1;
			total = t.f1[TOTAL_TRUE] + t.f1[TOTAL_FALSE];

			decisionThreshold = getRuntimeContext().hasBroadcastVariable(DECISION_THRESHOLD_BC_NAME)
				? getRuntimeContext(). <Double>getBroadcastVariable(DECISION_THRESHOLD_BC_NAME).get(0)
				: 0.5;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> values,
								 Collector <Tuple3 <Double, Long, Boolean>> out) throws Exception {
			for (Tuple3 <Double, Boolean, Double> t : values) {
				if (!isMiddlePoint(t, decisionThreshold)) {
					out.collect(Tuple3.of(t.f0, total - startIndex + 1, t.f1));
					startIndex++;
				}
			}
		}
	}

	static class CalcBinaryMetricsSummary
		extends RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, BaseMetricsSummary> {
		private static final long serialVersionUID = 5680342197308160013L;
		private Object[] labels;
		private long[] countValues;
		private boolean firstBin;
		private double auc;
		private double decisionThreshold;
		private double largestThreshold;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			AkPreconditions.checkState(list.size() > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
			labels = list.get(0).f1;

			List <BinaryPartitionSummary> statistics = getRuntimeContext()
				.getBroadcastVariable(PARTITION_SUMMARIES_BC_NAME);
			Tuple2 <Boolean, long[]> t = reduceBinaryPartitionSummary(statistics,
				getRuntimeContext().getIndexOfThisSubtask());
			firstBin = t.f0;
			countValues = t.f1;

			auc = getRuntimeContext(). <Tuple1 <Double>>getBroadcastVariable("auc").get(0).f0;
			long totalTrue = countValues[TOTAL_TRUE];
			long totalFalse = countValues[TOTAL_FALSE];
			if (totalTrue == 0) {
				LOG.warn("There is no positive sample in data!");
			}
			if (totalFalse == 0) {
				LOG.warn("There is no negative sample in data!");
			}
			if (totalTrue > 0 && totalFalse > 0) {
				auc = (auc - 1. * totalTrue * (totalTrue + 1) / 2) / (totalTrue * totalFalse);
			} else {
				auc = Double.NaN;
			}

			if (getRuntimeContext().hasBroadcastVariable(DECISION_THRESHOLD_BC_NAME)) {
				decisionThreshold = getRuntimeContext().
					<Double>getBroadcastVariable(DECISION_THRESHOLD_BC_NAME).get(0);
				largestThreshold = Double.POSITIVE_INFINITY;
			} else {
				decisionThreshold = 0.5;
				largestThreshold = 1.0;
			}
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> iterable,
								 Collector <BaseMetricsSummary> collector) {
			AccurateBinaryMetricsSummary summary =
				new AccurateBinaryMetricsSummary(labels, decisionThreshold, 0.0, 0L, auc);
			double[] tprFprPrecision = new double[RECORD_LEN];
			for (Tuple3 <Double, Boolean, Double> t : iterable) {
				updateAccurateBinaryMetricsSummary(
					t,
					summary,
					countValues,
					tprFprPrecision,
					firstBin,
					decisionThreshold,
					largestThreshold);
			}
			collector.collect(summary);
		}
	}
}
