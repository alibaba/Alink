package com.alibaba.alink.operator.common.evaluation;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * BaseEvaluationMetrics for classification evaluation.
 */
class ClassificationMetricComputers {

    interface BaseClassificationMetricComputer extends BiFunction<ConfusionMatrix, Integer, Double>, Serializable {}

    /**
     * TPR = TP / (TP + FN)
     */
    static class TruePositiveRate implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = matrix.numTruePositive(labelIndex) + matrix.numFalseNegative(labelIndex);
            return denominator == 0 ? 0.0 : matrix.numTruePositive(labelIndex) / denominator;
        }
    }

    /**
     * TNR = TN / (FP + TN)
     */
    static class TrueNegativeRate implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = matrix.numFalsePositive(labelIndex) + matrix.numTrueNegative(labelIndex);
            return denominator == 0 ? 0.0 : matrix.numTrueNegative(labelIndex) / denominator;
        }
    }

    /**
     * FPR = FP / (FP + TN)
     */
    static class FalsePositiveRate implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = matrix.numFalsePositive(labelIndex) + matrix.numTrueNegative(labelIndex);
            return denominator == 0 ? 0.0 : matrix.numFalsePositive(labelIndex) / denominator;
        }
    }

    /**
     * FNR = FN / (TP + FN)
     */
    static class FalseNegativeRate implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = matrix.numTruePositive(labelIndex) + matrix.numFalseNegative(labelIndex);
            return denominator == 0 ? 0.0 : matrix.numFalseNegative(labelIndex) / denominator;
        }
    }

    /**
     * p_a = (TP+TN)/total
     *
     * <p>p_e = ((TN+FP)(TN+FN)+(FN+TP)(FP+TP))/total/total
     *
     * <p>kappa = (p_a - p_e)/(1 - p_e)
     */
    static class Kappa implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double total = matrix.numFalseNegative(labelIndex) + matrix.numFalsePositive(labelIndex) + matrix
                .numTrueNegative(labelIndex)
                + matrix.numTruePositive(labelIndex);

            double pa = matrix.numTruePositive(labelIndex) + matrix.numTrueNegative(labelIndex);
            pa /= total;

            double pe = (matrix.numTruePositive(labelIndex) + matrix.numFalseNegative(labelIndex)) * (
                matrix.numTruePositive(labelIndex)
                    + matrix.numFalsePositive(labelIndex));
            pe += (matrix.numTrueNegative(labelIndex) + matrix.numFalsePositive(labelIndex)) * (matrix.numTrueNegative(
                labelIndex) + matrix.numFalseNegative(labelIndex));
            pe /= (total * total);
            if (pe < 1) {
                return (pa - pe) / (1 - pe);
            } else {
                return 1.0;
            }
        }
    }

    /**
     * PRECISION: TP / (TP + FP)
     */
    static class Precision implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = matrix.numTruePositive(labelIndex) + matrix.numFalsePositive(labelIndex);
            return denominator == 0 ? 0.0 : matrix.numTruePositive(labelIndex) / denominator;
        }
    }

    /**
     * F1: 2 * Precision * Recall / (Precision + Recall)
     *
     * <p>F1: 2 * TP / (2TP + FP + FN)
     */
    static class F1 implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double denominator = 2 * matrix.numTruePositive(labelIndex) + matrix.numFalsePositive(labelIndex) + matrix
                .numFalseNegative(labelIndex);
            return denominator == 0 ? 0.0 : 2 * matrix.numTruePositive(labelIndex) / denominator;
        }
    }

    /**
     * ACCURACY: (TP + TN) / Total
     */
    static class Accuracy implements BaseClassificationMetricComputer {
        @Override
        public Double apply(ConfusionMatrix matrix, Integer labelIndex) {
            double total = matrix.numFalseNegative(labelIndex) + matrix.numFalsePositive(labelIndex) + matrix
                .numTrueNegative(labelIndex)
                + matrix.numTruePositive(labelIndex);

            return (matrix.numTruePositive(labelIndex) + matrix.numTrueNegative(labelIndex)) / total;
        }
    }

}
