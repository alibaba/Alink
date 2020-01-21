package com.alibaba.alink.operator.common.evaluation;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for ConfusionMatrix
 */
public class ConfusionMatrixTest {

    @Test
    public void metricsTest() {
        long[][] matrix = new long[][]{ {2L, 0L}, {1L, 2L}};

        ConfusionMatrix cm = new ConfusionMatrix(new LongMatrix(matrix));
        long[][] expectMatrix = new long[][]{{2L, 0L}, {1L, 2L}};

        for(int i = 0; i < expectMatrix.length; i++){
            Assert.assertArrayEquals(expectMatrix[i], cm.longMatrix.getMatrix()[i]);
        }

        Assert.assertArrayEquals(cm.getActualLabelFrequency(), new long[]{3L, 2L});
        Assert.assertArrayEquals(cm.getPredictLabelFrequency(), new long[]{2L, 3L});
        
        double[] tp = new double[]{0.666, 1.0, 0.8, 0.833, 0.8};
        double[] tn = new double[]{1.0, 0.666, 0.866, 0.833, 0.8};
        double[] fp = new double[]{0.0, 0.333, 0.133, 0.166, 0.2};
        double[] fn = new double[]{0.333, 0.0, 0.199, 0.166, 0.2};
        double[] accuracy = new double[]{0.8, 0.8, 0.8, 0.8, 0.8};
        double[] precision = new double[]{1.0, 0.666, 0.866, 0.833, 0.8};
        double[] fMeasure = new double[]{0.8, 0.8, 0.8, 0.8, 0.8};
        double[] kappa = new double[]{0.615, 0.615, 0.615, 0.615, 0.6};
        
        for(int i = 0; i < tp.length; i++){
            Assert.assertEquals(kappa[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.KAPPA.computer, cm)[i], 0.001);
            Assert.assertEquals(tp[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.TRUE_POSITIVE.computer, cm)[i], 0.001);
            Assert.assertEquals(tn[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.TRUE_NEGATIVE.computer, cm)[i], 0.001);
            Assert.assertEquals(fp[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.FALSE_POSITIVE.computer, cm)[i], 0.001);
            Assert.assertEquals(fn[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.FALSE_NEGATIVE.computer, cm)[i], 0.001);
            Assert.assertEquals(accuracy[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.ACCURACY.computer, cm)[i], 0.001);
            Assert.assertEquals(precision[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.PRECISION.computer, cm)[i], 0.001);
            Assert.assertEquals(fMeasure[i], ClassificationEvaluationUtil.getAllValues(ClassificationEvaluationUtil.Computations.F1.computer, cm)[i], 0.001);
        }
    }
}