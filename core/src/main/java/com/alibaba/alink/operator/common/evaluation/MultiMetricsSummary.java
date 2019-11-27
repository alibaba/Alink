package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setClassificationCommonParams;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setLoglossParams;

/**
 * Save the evaluation data for multi classification.
 * <p>
 * The evaluation metrics include ACCURACY, PRECISION, RECALL, LOGLOSS, SENSITIVITY, SPECITIVITY and KAPPA.
 */
public final class MultiMetricsSummary implements BaseMetricsSummary<MultiClassMetrics, MultiMetricsSummary> {
    /**
     * Confusion matrix.
     */
    long[][] matrix;

    /**
     * Label array.
     */
    String[] labels;

    /**
     * The count of samples.
     */
    long total;

    /**
     * Logloss = sum_i{sum_j{y_ij * log(p_ij)}}
     */
    double logLoss;

    public MultiMetricsSummary(long[][] matrix, String[] labels, double logLoss, long total) {
        this.matrix = matrix;
        this.labels = labels;
        this.logLoss = logLoss;
        this.total = total;
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
        Preconditions.checkState(Arrays.equals(labels, multiClassMetrics.labels), "The labels are not the same!");

        int n = this.labels.length;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                this.matrix[i][j] += multiClassMetrics.matrix[i][j];
            }
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
        Params params = new Params();
        ConfusionMatrix data = new ConfusionMatrix(matrix);
        params.set(MultiClassMetrics.PREDICT_LABEL_FREQUENCY, data.getPredictLabelFrequency());
        params.set(MultiClassMetrics.PREDICT_LABEL_PROPORTION, data.getPredictLabelProportion());

        for (ClassificationEvaluationUtil.Computations c : ClassificationEvaluationUtil.Computations.values()) {
            params.set(c.arrayParamInfo, ClassificationEvaluationUtil.getAllValues(c.computer, data));
        }
        setClassificationCommonParams(params, data, labels);
        setLoglossParams(params, logLoss, total);
        return new MultiClassMetrics(params);
    }
}
