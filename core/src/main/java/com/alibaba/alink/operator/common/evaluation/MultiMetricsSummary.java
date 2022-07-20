package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkPreconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setClassificationCommonParams;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setLoglossParams;

/**
 * Save the evaluation data for multi classification.
 * <p>
 * The evaluation metrics include ACCURACY, PRECISION, RECALL, LOGLOSS, SENSITIVITY, SPECITIVITY and KAPPA.
 */
public final class MultiMetricsSummary implements BaseMetricsSummary <MultiClassMetrics, MultiMetricsSummary> {
	private static final long serialVersionUID = -8742985165888894890L;
	/**
	 * Confusion matrix.
	 */
	LongMatrix matrix;

	/**
	 * Label array.
	 */
	Object[] labels;

	/**
	 * The count of samples.
	 */
	long total;

	/**
	 * Logloss = sum_i{sum_j{y_ij * log(p_ij)}}
	 */
	double logLoss;

	public MultiMetricsSummary(long[][] matrix, Object[] labels, double logLoss, long total) {
		AkPreconditions.checkArgument(matrix.length > 0 && matrix.length == matrix[0].length,
			"The row size must be equal to col size!");
		this.matrix = new LongMatrix(matrix);
		this.labels = labels;
		this.logLoss = logLoss;
		this.total = total;
	}

	private void mergeConfusionMatrix(LongMatrix matrix, Object[] labels,
									  LongMatrix mergedMatrix, Map <Object, Integer> mergedLabelToIndex) {
		for (int i = 0; i < matrix.getRowNum(); i += 1) {
			int newI = mergedLabelToIndex.get(labels[i]);
			for (int j = 0; j < matrix.getColNum(); j += 1) {
				int newJ = mergedLabelToIndex.get(labels[j]);
				mergedMatrix.setValue(newI, newJ, mergedMatrix.getValue(newI, newJ) + matrix.getValue(i, j));
			}
		}
	}

	/**
	 * Merge the confusion matrix, and add the logLoss.
	 *
	 * @param multiClassMetrics the MultiMetricsSummary to merge.
	 * @return the merged result.
	 */
	@Override
	public MultiMetricsSummary merge(MultiMetricsSummary multiClassMetrics) {
		if (null == multiClassMetrics) {
			return this;
		}
		if (Arrays.equals(labels, multiClassMetrics.labels)) {
			this.matrix.plusEqual(multiClassMetrics.matrix);
		} else {
			// Merge labels from two MultiMetricsSummary instances
			HashSet <Object> allLabelSet = new HashSet <>();
			allLabelSet.addAll(Arrays.asList(labels));
			allLabelSet.addAll(Arrays.asList(multiClassMetrics.labels));
			Object[] mergedLabels = allLabelSet.toArray();
			Arrays.sort(mergedLabels, Collections.reverseOrder());

			int numMergedLabels = mergedLabels.length;
			Map <Object, Integer> mergedLabelToIndex = IntStream.range(0, numMergedLabels)
				.boxed()
				.collect(Collectors. <Integer, Object, Integer>toMap(d -> mergedLabels[d], d -> d));

			// Merge confusion matrix from two MultiMetricsSummary instances
			LongMatrix mergedMatrix = new LongMatrix(new long[numMergedLabels][numMergedLabels]);
			mergeConfusionMatrix(matrix, labels, mergedMatrix, mergedLabelToIndex);
			mergeConfusionMatrix(multiClassMetrics.matrix, multiClassMetrics.labels, mergedMatrix, mergedLabelToIndex);

			// Re-assign labels and matrix of this
			this.labels = mergedLabels;
			this.matrix = mergedMatrix;
		}
		this.logLoss += multiClassMetrics.logLoss;
		this.total += multiClassMetrics.total;
		return this;
	}

	/**
	 * Calculate the detail info based on the confusion matrix.
	 */
	@Override
	public MultiClassMetrics toMetrics() {
		String[] labelStrs = new String[labels.length];
		for (int i = 0; i < labels.length; i++) {
			labelStrs[i] = labels[i].toString();
		}
		Params params = new Params();
		ConfusionMatrix data = new ConfusionMatrix(matrix);
		params.set(MultiClassMetrics.PREDICT_LABEL_FREQUENCY, data.getPredictLabelFrequency());
		params.set(MultiClassMetrics.PREDICT_LABEL_PROPORTION, data.getPredictLabelProportion());

		for (ClassificationEvaluationUtil.Computations c : ClassificationEvaluationUtil.Computations.values()) {
			params.set(c.arrayParamInfo, ClassificationEvaluationUtil.getAllValues(c.computer, data));
		}
		setClassificationCommonParams(params, data, labelStrs);
		setLoglossParams(params, logLoss, total);
		return new MultiClassMetrics(params);
	}
}
