package com.alibaba.alink.operator.common.evaluation;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for curves.
 */
public class BaseEvaluationCurveTest {

    @Test
    public void calcArea() {
        EvaluationCurvePoint[] curvePoints = new EvaluationCurvePoint[11];

        for(int i = 0; i < 11; i++){
            curvePoints[i] = new EvaluationCurvePoint((double)i / 10, (double)i / 100, (double)i / 10);
        }
        EvaluationCurve curve = new EvaluationCurve(curvePoints);
        Assert.assertEquals(0.05, curve.calcArea(), 0.001);
    }

    @Test
    public void calcKs() {
        EvaluationCurvePoint[] curvePoints = new EvaluationCurvePoint[11];

        for(int i = 0; i < 11; i++){
            curvePoints[i] = new EvaluationCurvePoint((double)i / 10, (double)i / 100, (double)i / 10);
        }
        EvaluationCurve curve = new EvaluationCurve(curvePoints);

        Assert.assertEquals(0.9, curve.calcKs(), 0.001);
    }
}