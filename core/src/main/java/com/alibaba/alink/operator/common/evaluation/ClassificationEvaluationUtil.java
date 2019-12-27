package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.params.evaluation.MultiEvaluationParams;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
    static final Tuple2<String, Integer> WINDOW = Tuple2.of("window", 0);
    static final Tuple2<String, Integer> ALL = Tuple2.of("all", 1);

    /**
     * Divide [0,1] into <code>DETAIL_BIN_NUMBER</code> bins.
     */
    public static int DETAIL_BIN_NUMBER = 100000;

    /**
     * Binary Classification label number.
     */
    static int BINARY_LABEL_NUMBER = 2;

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
    static Type judgeEvaluationType(Params params) {
        Type type;
        if (params.contains(MultiEvaluationParams.PREDICTION_DETAIL_COL)) {
            type = Type.PRED_DETAIL;
        } else if (params.contains(MultiEvaluationParams.PREDICTION_COL)) {
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
    public static Tuple2<Map<String, Integer>, String[]> buildLabelIndexLabelArray(HashSet<String> set,
                                                                                   boolean binary,
                                                                                   String positiveValue) {
        String[] labels = set.toArray(new String[0]);
        Arrays.sort(labels, Collections.reverseOrder());

        Preconditions.checkArgument(labels.length >= 2, "The distinct label number less than 2!");
        Preconditions.checkArgument(!binary || labels.length == BINARY_LABEL_NUMBER,
            "The number of labels must be equal to 2!");
        Map<String, Integer> map = new HashMap<>(labels.length);
        if (binary && null != positiveValue) {
            if (labels[1].equals(positiveValue)) {
                labels[1] = labels[0];
                labels[0] = positiveValue;
            } else if (!labels[0].equals(positiveValue)) {
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
        ParamInfo<double[]> arrayParamInfo;
        ParamInfo<Double> weightedParamInfo;
        ParamInfo<Double> macroParamInfo;
        ParamInfo<Double> microParamInfo;

        Computations(ClassificationMetricComputers.BaseClassificationMetricComputer computer,
                     ParamInfo<double[]> paramInfo,
                     ParamInfo<Double> weightedParamInfo,
                     ParamInfo<Double> macroParamInfo,
                     ParamInfo<Double> microParamInfo) {
            this.computer = computer;
            this.arrayParamInfo = paramInfo;
            this.weightedParamInfo = weightedParamInfo;
            this.macroParamInfo = macroParamInfo;
            this.microParamInfo = microParamInfo;
        }
    }
}
