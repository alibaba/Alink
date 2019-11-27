package com.alibaba.alink.operator.common.linear.objFunc;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.AftRegObjFunc;
import com.alibaba.alink.params.regression.AftRegTrainParams;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AftObjFuncTest {
    private final static int FDIM = 10;
    private final static double EPS = 1.0e-16;

    @Test
    public void calcSearchValues() {
        AftRegObjFunc objfunc = new AftRegObjFunc(new Params()
                .set(AftRegTrainParams.L_1, 0.1)
                .set(AftRegTrainParams.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
            dirVec.set(i, 0.1);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.01 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        double[] trueValues = new double[]{
                1858.0906639,
                1854.4931528,
                1846.4415772,
                1833.475521,
                1815.0990642,
                1790.7783532,
                1759.9390144,
                1721.9634027,
                1676.1876744,
                1621.8986742};

        double[] losses = objfunc.calcSearchValues(
                labelVectors, coef, dirVec, 0.5, 10);
        DecimalFormat format = new DecimalFormat("0.0000000");

        for (int i = 0; i < FDIM; ++i) {
            losses[i] = Double.valueOf(format.format(losses[i]));
            assertEquals(losses[i], trueValues[i], EPS);
        }
    }

    @Test
    public void calcObjValues() {
        AftRegObjFunc objfunc = new AftRegObjFunc(new Params()
                .set(AftRegTrainParams.L_1, 0.1)
                .set(AftRegTrainParams.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
            dirVec.set(i, 0.1);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.01 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        double[] trueConstValues = new double[]{
                1858.0906639,
                1854.4931528,
                1846.4415772,
                1833.475521,
                1815.0990642,
                1790.7783532,
                1759.9390144,
                1721.9634027,
                1676.1876744,
                1621.8986742};

        double[] closses = objfunc.constraintCalcSearchValues(
                labelVectors, coef, dirVec, 0.5, 10);
        DecimalFormat format = new DecimalFormat("0.0000000");

        for (int i = 0; i < FDIM; ++i) {
            closses[i] = Double.valueOf(format.format(closses[i]));

            assertEquals(closses[i], trueConstValues[i], EPS);
        }
    }

    @Test
    public void calcGrad() {
        AftRegObjFunc objfunc = new AftRegObjFunc(new Params()
                .set(AftRegTrainParams.L_1, 0.1)
                .set(AftRegTrainParams.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
            dirVec.set(i, 0.1);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.01 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }
        DecimalFormat format = new DecimalFormat("0.0000000");
        Tuple2<Double, Double> objValue = objfunc.calcObjValue(labelVectors, coef);
        assertTrue(Double.valueOf(format.format(objValue.f0)) == 20.7187566);
        double[] trueGrad = new double[]{
                0.4664272,
                0.8046436,
                1.1428601,
                1.4810766,
                1.8192931,
                2.1575096,
                2.495726,
                2.8339425,
                3.172159,
                -12.9805304};

        DenseVector grad = new DenseVector(coef.size());
        double weight = objfunc.calcGradient(labelVectors, coef, grad);

        for (int i = 0; i < FDIM; ++i) {
            double val =  Double.valueOf(format.format(grad.get(i)));
            assertEquals(val, trueGrad[i], EPS);
        }
    }

    @Test
    public void calcHessian() {
        AftRegObjFunc objfunc = new AftRegObjFunc(new Params()
                .set(AftRegTrainParams.L_1, 1.0e-2)
                .set(AftRegTrainParams.L_2, 1.0e-2));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
            dirVec.set(i, 0.1);
        }
        //100个数据，每个数据10个维度
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.01 * i);
            }
            //前两个可能是权重和label。
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }
        //这两个应该是初始的。
        DenseMatrix hessian = new DenseMatrix(coef.size(), coef.size());
        DenseVector grad = new DenseVector(coef.size());
        Tuple2<Double,Double> loss = objfunc.calcHessianGradientLoss(labelVectors, coef, hessian, grad);
        double[] trueMat = new double[]{
            2.0000000510094984,
            2.000000737376146,
            2.000002340193193,
            2.00000485946064,
            2.000008295178487,
            2.0000126473467335,
            2.0000179159653797,
            2.000024101034426,
            2.000031202553872,
            1751.091754655224};
        for (int i = 0; i < FDIM; ++i) {
            assertEquals(hessian.get(i,i), trueMat[i], EPS);
        }
    }
}
