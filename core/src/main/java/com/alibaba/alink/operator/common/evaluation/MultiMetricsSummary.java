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
    LongMatrix matrix;

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
        Preconditions.checkArgument(matrix.length > 0 && matrix.length == matrix[0].length,
            "The row size must be equal to col size!");
        this.matrix = new LongMatrix(matrix);
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
        this.matrix.plusEqual(multiClassMetrics.matrix);
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
