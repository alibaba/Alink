package com.alibaba.alink.operator.common.linear.objFunc;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.SoftmaxObjFunc;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SoftmaxObjFuncTest {
    private final static int FDIM = 10;
    private final static double EPS = 1.0e-16;

    @Test
    public void calcSearchValues() throws Exception {
        SoftmaxObjFunc objfunc = new SoftmaxObjFunc(new Params()
            .set(HasL1.L_1, 0.1)
            .set(HasL2.L_2, 0.1)
            .set(ModelParamName.NUM_CLASSES, 3));

        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.1 * i);
            dirVec.set(i, 0.5);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        double[] trueValues = new double[] {
            0.030403516448075152,
            0.03040384780510408,
            0.03040847327743279,
            0.030475957193068837,
            0.03153435656977166,
            0.05059162113327886,
            0.5457886921018531,
            118.42109132310769,
            943.4408732404858,
            1811.2962418211239};

        double[] losses = objfunc.calcSearchValues(
            labelVectors, coef, dirVec, 0.5, 10);

        for (int i = 0; i < FDIM; ++i) {
            assertEquals(losses[i], trueValues[i], EPS);
        }
    }

    @Test
    public void calcObjValues() throws Exception {
        SoftmaxObjFunc objfunc = new SoftmaxObjFunc(new Params()
            .set(HasL1.L_1, 0.1)
            .set(HasL2.L_2, 0.1)
            .set(ModelParamName.NUM_CLASSES, 3));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.1 * i);
            dirVec.set(i, 0.5);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        Tuple2<Double, Double> objValue = objfunc.calcObjValue(labelVectors, coef);
        assertEquals(objValue.f0, 3.6353040351644808, EPS);
    }

    @Test
    public void calcGrad() throws Exception {
        SoftmaxObjFunc objfunc = new SoftmaxObjFunc(new Params()
            .set(HasL1.L_1, 0.1)
            .set(HasL2.L_2, 0.1)
            .set(ModelParamName.NUM_CLASSES, 3));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.1 * i);
            dirVec.set(i, 0.5);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        double[] trueGrad = new double[] {
            0.3001070700605414,
            0.32041053188156643,
            0.34071399370259137,
            0.3610174555236164,
            0.38132091734464146,
            0.3998929299196802,
            0.4195894678341977,
            0.43928600574871535,
            0.4589825436632329,
            0.4786790815777505
        };

        DenseVector grad = new DenseVector(coef.size());
        double weight = objfunc.calcGradient(labelVectors, coef, grad);

        for (int i = 0; i < FDIM; ++i) {
            assertEquals(grad.get(i), trueGrad[i], EPS);
        }
    }

    @Test
    public void calcHessian() throws Exception {
        SoftmaxObjFunc objfunc = new SoftmaxObjFunc(new Params()
            .set(HasL1.L_1, 0.1)
            .set(HasL2.L_2, 0.1)
            .set(ModelParamName.NUM_CLASSES, 3));
        List<Tuple3<Double, Double, Vector>> labelVectors = new ArrayList<>();
        DenseVector coef = new DenseVector(FDIM);
        DenseVector dirVec = new DenseVector(FDIM);
        for (int i = 0; i < FDIM; ++i) {
            coef.set(i, 1.0 + 0.1 * i);
            dirVec.set(i, 0.5);
        }
        for (int i = 0; i < 100; ++i) {
            DenseVector tmp = new DenseVector(FDIM);
            for (int j = 0; j < FDIM; ++j) {
                tmp.set(j, j + 0.1 * i);
            }
            labelVectors.add(Tuple3.of(1.0, 1.0, tmp));
        }

        DenseMatrix hessian = new DenseMatrix(coef.size(), coef.size());
        DenseVector grad = new DenseVector(coef.size());
        Tuple2<Double,Double> loss = objfunc.calcHessianGradientLoss(labelVectors, coef, hessian, grad);
        double[] trueMat = new double[] {
            20.008609951451433,
            20.060220545670703,
            20.172295036435074,
            20.34483342374455,
            20.577835707599135,
            20.008609951941853,
            20.060220576228186,
            20.172295149343217,
            20.344833671286942,
            20.57783614205936
        };
        for (int i = 0; i < FDIM; ++i) {
            assertEquals(hessian.get(i,i), trueMat[i], EPS);
        }
    }
}
