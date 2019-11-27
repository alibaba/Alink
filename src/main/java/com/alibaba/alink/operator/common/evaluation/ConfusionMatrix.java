package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Confusion matrix for classification evaluation.
 *
 * <p>The horizontal axis is predictResult value, the vertical axis is label value.
 *
 * <p>[TP FP][FN TN].
 *
 * <p>Calculate other metrics based on the confusion matrix.
 */
public class ConfusionMatrix implements Serializable {
    /**
     * Record the matrix data.
     */
    long[][] matrix;

    /**
     * The number of labels.
     */
    int labelCnt;

    /**
     * The sum of the matrix data.
     */
    long total;

    /**
     * PredictLabelFrequency records the frequency of each label in the prediction result. It's also the sum of each
     * row.
     */
    private long[] actualLabelFrequency;

    /**
     * ActualLabelFrequency records the actual frequency of each label. It's also the sum of each column.
     */
    private long[] predictLabelFrequency;

    /**
     * Record the sum of TruePositive/TrueNegative/FalsePositive/FalseNegative of all the labels.
     */
    private double tpCount = 0.0, tnCount = 0.0, fpCount = 0.0, fnCount = 0.0;

    public ConfusionMatrix(long[][] matrix) {
        Preconditions.checkState(matrix.length > 0 && matrix.length == matrix[0].length,
            "The number of row and column of the matrix must be the same!");
        this.matrix = matrix;
        labelCnt = matrix.length;
        actualLabelFrequency = new long[labelCnt];
        predictLabelFrequency = new long[labelCnt];
        for (int i = 0; i < labelCnt; i++) {
            for (int j = 0; j < labelCnt; j++) {
                predictLabelFrequency[i] += matrix[i][j];
                actualLabelFrequency[i] += matrix[j][i];
                total += matrix[i][j];
            }
        }
        for (int i = 0; i < labelCnt; i++) {
            tnCount += numTrueNegative(i);
            tpCount += numTruePositive(i);
            fnCount += numFalseNegative(i);
            fpCount += numFalsePositive(i);
        }
    }

    long[] getActualLabelFrequency() {
        return actualLabelFrequency;
    }

    double[] getActualLabelProportion() {
        double[] proportion = new double[labelCnt];
        for (int i = 0; i < labelCnt; i++) {
            proportion[i] = (double)actualLabelFrequency[i] / (double)total;
        }
        return proportion;
    }

    long[] getPredictLabelFrequency() {
        return predictLabelFrequency;
    }

    double[] getPredictLabelProportion() {
        double[] proportion = new double[labelCnt];
        for (int i = 0; i < labelCnt; i++) {
            proportion[i] = (double)predictLabelFrequency[i] / (double)total;
        }
        return proportion;
    }

    /**
     * Return the overall kappa.
     */
    double getTotalKappa() {
        double pa = 0, pe = 0;
        for (int i = 0; i < labelCnt; i++) {
            pe += (predictLabelFrequency[i] * actualLabelFrequency[i]);
            pa += matrix[i][i];
        }
        pe /= (total * total);
        pa /= total;

        if (pe < 1) {
            return (pa - pe) / (1 - pe);
        } else {
            return 1.0;
        }
    }

    /**
     * Return the overall accuracy.
     */
    double getTotalAccuracy() {
        double pa = 0;
        for (int i = 0; i < labelCnt; i++) {
            pa += matrix[i][i];
        }
        return pa / total;
    }

    double numTruePositive(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? tpCount : matrix[labelIndex][labelIndex];
    }

    double numTrueNegative(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? tnCount : matrix[labelIndex][labelIndex] + total - predictLabelFrequency[labelIndex]
            - actualLabelFrequency[labelIndex];
    }

    double numFalsePositive(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? fpCount : predictLabelFrequency[labelIndex] - matrix[labelIndex][labelIndex];
    }

    double numFalseNegative(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? fnCount : actualLabelFrequency[labelIndex] - matrix[labelIndex][labelIndex];
    }
}
