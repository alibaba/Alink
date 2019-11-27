package com.alibaba.alink.operator.common.linear.objFunc;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.UnaryLossObjFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnaryObjFuncTest {
    private final static int FEATURE_DIM = 10;
    private final static double EPS = 1.0e-16;

    @Test
    public void calcSearchValues() throws Exception {
        UnaryLossObjFunc objfunc = new UnaryLossObjFunc(new LogLossFunc(),
            new Params().set(HasL1.L_1, 0.1)
                .set(HasL2.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FEATURE_DIM);
        DenseVector dirVec = new DenseVector(FEATURE_DIM);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            coef.set(i, 1.0 + 0.5 * i);
            dirVec.set(i, (i + 0.5));
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FEATURE_DIM);
            for (int j = 0; j < FEATURE_DIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0 - i % 2, tmp));
        }

        double[] trueValues = new double[] {
            34.65735902799723,
            10322.157359028002,
            37947.15735902796,
            65572.15735902794,
            93197.15735902794,
            120822.15735902793,
            148447.157359028,
            176072.15735902806,
            203697.15735902812,
            231322.15735902818};

        double[] losses = objfunc.calcSearchValues(
            labelVectors, coef, dirVec, 1.0, 10);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            assertEquals(losses[i], trueValues[i], EPS);
        }
    }

    @Test
    public void calcConstraintSearchValues() throws Exception {
        UnaryLossObjFunc objfunc = new UnaryLossObjFunc(new LogLossFunc(),
            new Params().set(HasL1.L_1, 0.1)
                .set(HasL2.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FEATURE_DIM);
        DenseVector dirVec = new DenseVector(FEATURE_DIM);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            coef.set(i, 1.0 + 0.5 * i);
            dirVec.set(i, (i + 0.1));
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FEATURE_DIM);
            for (int j = 0; j < FEATURE_DIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0 - i % 2, tmp));
        }

        double[] trueConstValues = new double[] {
            34.65735902799723,
            34.65735902799723,
            37.163138901642334,
            39.847755908478675,
            40.14874960561338,
            40.48932608213041,
            40.87755215903219,
            41.323690786508514,
            41.84091851878743,
            42.44629788093937};

        double[] constraintLosses = objfunc.constraintCalcSearchValues(
            labelVectors, coef, dirVec, 0.5, 10);

        for (int i = 0; i < FEATURE_DIM; ++i) {
            assertEquals(constraintLosses[i], trueConstValues[i], EPS);
        }
    }

    @Test
    public void calcObjValues() throws Exception {
        UnaryLossObjFunc objfunc = new UnaryLossObjFunc(new LogLossFunc(),
            new Params().set(HasL1.L_1, 0.1)
                .set(HasL2.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FEATURE_DIM);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            coef.set(i, 1.0 + 0.5 * i);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FEATURE_DIM);
            for (int j = 0; j < FEATURE_DIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0 - i % 2, tmp));
        }

        Tuple2<Double, Double> objValue = objfunc.calcObjValue(labelVectors, coef);
        assertEquals(objValue.f0, 16.221573590279974, EPS);
    }

    @Test
    public void calcGrad() throws Exception {
        UnaryLossObjFunc objfunc = new UnaryLossObjFunc(new LogLossFunc(),
            new Params().set(HasL1.L_1, 0.1)
                .set(HasL2.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FEATURE_DIM);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FEATURE_DIM);
            for (int j = 0; j < FEATURE_DIM; ++j) {
                tmp.set(j, 1.0 + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(i * 0.1, 1.0 * (i % 2), tmp));
        }
        double[] trueGrad = new double[] {
            0.29999999645330994,
            0.30199999645330994,
            0.30399999645330994,
            0.30599999645330994,
            0.30799999645330994,
            0.30999999645330995,
            0.31199999645330995,
            0.31399999645330995,
            0.31599999645330995,
            0.31799999645330995};
        DenseVector grad = new DenseVector(coef.size());
        double weight = objfunc.calcGradient(labelVectors, coef, grad);

        for (int i = 0; i < FEATURE_DIM; ++i) {
            assertEquals(grad.get(i), trueGrad[i], EPS);
        }

    }

    @Test
    public void calcHessian() throws Exception {
        UnaryLossObjFunc objfunc = new UnaryLossObjFunc(new LogLossFunc(),
            new Params().set(HasL1.L_1, 0.1)
                .set(HasL2.L_2, 0.1));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FEATURE_DIM);
        for (int i = 0; i < FEATURE_DIM; ++i) {
            coef.set(i, 1.0 + 0.01 * i);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FEATURE_DIM);
            for (int j = 0; j < FEATURE_DIM; ++j) {
                tmp.set(j, 1.0 + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(i * 0.1, 1.0 * (i % 2), tmp));
        }

        DenseMatrix hessian = new DenseMatrix(FEATURE_DIM, FEATURE_DIM);
        DenseVector grad = new DenseVector(FEATURE_DIM);
        Tuple2<Double,Double> loss = objfunc.calcHessianGradientLoss(labelVectors, coef, hessian, grad);
        double[] trueMat = new double[] {
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605,
            99.0000020939605};
        for (int i = 0; i < FEATURE_DIM; ++i) {
            assertEquals(hessian.get(i, i), trueMat[i], EPS);
        }
    }
}
