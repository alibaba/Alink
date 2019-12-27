package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.extractMatrixThreCurve;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.getMiddleThresholdIndex;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.sample;

/**
 * Unit test for BinaryClassMetrics.
 */
public class BinaryClassMetricsTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void reduceTest(){
        BinaryMetricsSummary metrics1 = new BinaryMetricsSummary(new long[] {0, 1}, new long[] {2, 3},
            new String[] {"0", "1"}, 0.5, 6);
        BinaryMetricsSummary metrics2 = new BinaryMetricsSummary(new long[] {2, 3}, new long[] {1, 0},
            new String[] {"0", "1"}, 1.2, 6);
        BinaryMetricsSummary metrics = metrics1.merge(metrics2);

        Assert.assertEquals(12, metrics.total);
        Assert.assertEquals(1.7, metrics.logLoss, 0.01);
        Assert.assertArrayEquals(new String[] {"0", "1"}, metrics.labels);
        Assert.assertArrayEquals(new long[] {2, 4}, metrics.positiveBin);
        Assert.assertArrayEquals(new long[] {3, 3}, metrics.negativeBin);

        BinaryMetricsSummary baseMetricsSummary = metrics1.merge(null);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The labels are not the same!");
        metrics2 = new BinaryMetricsSummary(new long[] {2, 3}, new long[] {1, 0}, new String[] {"1", "0"}, 1.2, 6);
        metrics1.merge(metrics2);
    }

    @Test
    public void saveAsParamsTest() {
        long[] bin0 = new long[100000];
        bin0[70000] = 1;
        bin0[80000] = 1;
        bin0[90000] = 1;

        long[] bin1 = new long[100000];
        bin1[60000] = 1;
        bin1[75000] = 1;

        BinaryMetricsSummary metricsSummary = new BinaryMetricsSummary(bin0, bin1, new String[] {"0", "1"}, 2.987, 5);
        BinaryClassMetrics metrics = metricsSummary.toMetrics();

        Assert.assertEquals(metrics.getPrc(), 0.9027777777777777, 0.01);
        Assert.assertEquals(metrics.getMacroRecall(), 0.5, 0.01);
        Assert.assertEquals(metrics.getMacroSpecificity(), 0.5, 0.01);
        Assert.assertEquals(metrics.getAuc(), 0.8333333333333333, 0.01);
        Assert.assertEquals(metrics.getMacroAccuracy(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMicroFalseNegativeRate(), 0.4, 0.01);
        Assert.assertEquals(metrics.getWeightedRecall(), 0.6, 0.01);
        Assert.assertEquals(metrics.getWeightedPrecision(), 0.36, 0.01);
        Assert.assertEquals(metrics.getMacroPrecision(), 0.3, 0.01);
        Assert.assertEquals(metrics.getMicroTruePositiveRate(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMacroKappa(), 0.0, 0.01);
        Assert.assertEquals(metrics.getMicroSpecificity(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMacroF1(), 0.37499999999999994, 0.01);
        Assert.assertEquals(metrics.getWeightedKappa(), 0.0, 0.01);
        Assert.assertEquals(metrics.getWeightedTruePositiveRate(), 0.6, 0.01);
        Assert.assertEquals(metrics.getTotalSamples(), 5, 0.01);
        Assert.assertEquals(metrics.getMicroTrueNegativeRate(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMicroSensitivity(), 0.6, 0.01);
        Assert.assertEquals(metrics.getWeightedAccuracy(), 0.6, 0.01);
        Assert.assertEquals(metrics.getKs(), 0.6666666666666666, 0.01);
        Assert.assertEquals(metrics.getAccuracy(), 0.6, 0.01);
        Assert.assertEquals(metrics.getWeightedFalseNegativeRate(), 0.4, 0.01);
        Assert.assertEquals(metrics.getMicroF1(), 0.6, 0.01);
        Assert.assertEquals(metrics.getWeightedSpecificity(), 0.4, 0.01);
        Assert.assertEquals(metrics.getWeightedF1(), 0.4499999999999999, 0.01);
        Assert.assertEquals(metrics.getMicroAccuracy(), 0.6, 0.01);
        Assert.assertEquals(metrics.getWeightedTrueNegativeRate(), 0.4, 0.01);
        Assert.assertEquals(metrics.getKappa(), 0.0, 0.01);
        Assert.assertEquals(metrics.getMacroSensitivity(), 0.5, 0.01);
        Assert.assertEquals(metrics.getWeightedSensitivity(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMicroRecall(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMicroFalsePositiveRate(), 0.4, 0.01);
        Assert.assertEquals(metrics.getWeightedFalsePositiveRate(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMicroPrecision(), 0.6, 0.01);
        Assert.assertEquals(metrics.getMacroTrueNegativeRate(), 0.5, 0.01);
        Assert.assertEquals(metrics.getMicroKappa(), 0.19999999999999996, 0.01);
    }

    @Test
    public void extractMatrixThreCurveTest(){
        long[] bin0 = new long[100000];
        bin0[70000] = 1;
        bin0[80000] = 1;
        bin0[90000] = 1;

        long[] bin1 = new long[100000];
        bin1[60000] = 1;
        bin1[75000] = 1;

        Tuple3<ConfusionMatrix[], double[], EvaluationCurve[]> res = extractMatrixThreCurve(bin0, bin1, 5);
        EvaluationCurve[] curve = res.f2;
        ConfusionMatrix[] data = res.f0;
        double[] threshold = res.f1;

        double[] expectThre = new double[] {1.0, 0.9, 0.8, 0.75, 0.7, 0.6, 0.5};

        for (int i = 0; i < expectThre.length; i++) {
            Assert.assertEquals(expectThre[i], threshold[i], 0.001);
        }

        long[][] expectMatrix = new long[][] {{0L, 0L}, {3L, 2L}, {1L, 0L}, {2L, 2L},
            {2L, 0L}, {1L, 2L}, {2L, 1L}, {1L, 1L}, {3L, 1L},
            {0L, 1L}, {3L, 2L}, {0L, 0L}, {3L, 2L}, {0L, 0L}};

        int cnt = 0;
        for (ConfusionMatrix aData : data) {
            for (int j = 0; j < aData.longMatrix.getRowNum(); j++) {
                Assert.assertArrayEquals(expectMatrix[cnt++], aData.longMatrix.getMatrix()[j]);
            }
        }

        double[][] expectCurve = new double[][] {
            {0.0, 0.0, 0.0, 0.5, 0.5, 1.0, 1.0},
            {0.0, 0.333, 0.666, 0.666, 1.0, 1.0, 1.0},
            {0.0, 0.333, 0.666, 0.666, 1.0, 1.0, 1.0},
            {1.0, 1.0, 1.0, 0.666, 0.75, 0.6, 0.6},
            {0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.0},
            {0.0, 1.0, 2.0, 2.0, 3.0, 3.0, 3.0}};

        cnt = 0;
        for (EvaluationCurve aCurve : curve) {
            double[][] actual = aCurve.getXYArray();
            for (int j = 0; j < expectCurve[0].length; j++) {
                Assert.assertEquals(expectCurve[cnt][j], actual[0][j], 0.001);
                Assert.assertEquals(expectCurve[cnt + 1][j], actual[1][j], 0.001);
            }
            cnt += 2;
        }
    }

    @Test
    public void getMiddleThresholdIndexTest(){
        double[] threshold = new double[] {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        Assert.assertEquals(4, getMiddleThresholdIndex(threshold));
    }

    @Test
    public void sampleTest(){
        EvaluationCurve[] curves = new EvaluationCurve[3];
        ConfusionMatrix[] data = new ConfusionMatrix[7];

        double[] threshold = new double[] {1.0, 0.9, 0.8, 0.75, 0.7, 0.6, 0.5};

        long[][] matrix = new long[][] {{0L, 0L}, {3L, 2L}, {1L, 0L}, {2L, 2L}, {2L, 0L}, {1L, 2L}, {2L, 1L}, {1L, 1L},
            {3L, 1L},
            {0L, 1L}, {3L, 2L}, {0L, 0L}, {3L, 2L}, {0L, 0L}};

        int cnt = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = new ConfusionMatrix(new long[][] {matrix[cnt], matrix[cnt + 1]});
            cnt += 2;
        }

        double[][] curve = new double[][] {
            {0.0, 0.0, 0.0, 0.5, 0.5, 1.0, 1.0},
            {0.0, 0.333, 0.666, 0.666, 1.0, 1.0, 1.0},
            {0.0, 0.333, 0.666, 0.666, 1.0, 1.0, 1.0},
            {1.0, 1.0, 1.0, 0.666, 0.75, 0.6, 0.6},
            {0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0},
            {0.0, 1.0, 2.0, 2.0, 3.0, 3.0, 3.0}};

        cnt = 0;
        for (int i = 0; i < curves.length; i++) {
            EvaluationCurvePoint[] curvePoints = new EvaluationCurvePoint[7];
            for (int j = 0; j < threshold.length; j++) {
                curvePoints[j] = new EvaluationCurvePoint(curve[cnt][j], curve[cnt + 1][j], threshold[j]);
            }
            curves[i] = new EvaluationCurve(curvePoints);

            cnt += 2;
        }

        Tuple3<ConfusionMatrix[], double[], EvaluationCurve[]> res = Tuple3.of(data, threshold, curves);
        Tuple3<ConfusionMatrix[], double[], EvaluationCurve[]> sampleRes = sample(0.1, res);
        double[] expect = new double[] {0.9, 0.8, 0.7, 0.6, 0.5};
        Assert.assertEquals(sampleRes.f0.length, expect.length);
        for (int i = 0; i < expect.length; i++) {
            Assert.assertEquals(expect[i], sampleRes.f1[i], 0.001);
        }
    }
}