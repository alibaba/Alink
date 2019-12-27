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
    LongMatrix longMatrix;

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

    public ConfusionMatrix(long[][] matrix){
        this(new LongMatrix(matrix));
    }

    public ConfusionMatrix(LongMatrix longMatrix) {
        Preconditions.checkArgument(longMatrix.getRowNum() == longMatrix.getColNum(),
            "The row size must be equal to col size!");
        this.longMatrix = longMatrix;
        labelCnt = this.longMatrix.getRowNum();
        actualLabelFrequency = longMatrix.getColSums();
        predictLabelFrequency = longMatrix.getRowSums();
        total = longMatrix.getTotal();
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
            pa += longMatrix.getValue(i, i);
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
            pa += longMatrix.getValue(i, i);
        }
        return pa / total;
    }

    double numTruePositive(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? tpCount : longMatrix.getValue(labelIndex, labelIndex);
    }

    double numTrueNegative(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? tnCount : longMatrix.getValue(labelIndex, labelIndex) + total - predictLabelFrequency[labelIndex]
            - actualLabelFrequency[labelIndex];
    }

    double numFalsePositive(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? fpCount : predictLabelFrequency[labelIndex] - longMatrix.getValue(labelIndex, labelIndex);
    }

    double numFalseNegative(Integer labelIndex) {
        Preconditions.checkArgument(null == labelIndex || labelIndex < labelCnt,
            "labelIndex must be null or less than " + labelCnt);
        return null == labelIndex ? fnCount : actualLabelFrequency[labelIndex] - longMatrix.getValue(labelIndex, labelIndex);
    }
}
