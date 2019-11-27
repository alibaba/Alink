package com.alibaba.alink.operator.common.optim.objfunc;

import java.io.Serializable;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Abstract object function for optimization. This class provides the function api to calculate gradient, loss, hessian
 * of object function. It also provides functions to help you update gradient and hessian with samples one by one.
 */
public abstract class OptimObjFunc implements Serializable {

    protected final double l1;
    protected final double l2;
    protected Params params;

    /**
     * Constructor.
     *
     * @param params Input parameters.
     */
    public OptimObjFunc(Params params) {
        if (null == params) {
            this.params = new Params();
        } else {
            this.params = params;
        }
        this.l1 = this.params.get(HasL1.L_1);
        this.l2 = this.params.get(HasL2.L_2);
    }

    public double getL1() {
        return l1;
    }

    public double getL2() {
        return l2;
    }

    /**
     * Calculate loss.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return the loss value
     */
    protected abstract double calcLoss(Tuple3<Double, Double, Vector> labelVector,
                                       DenseVector coefVector);

    /**
     * Update gradient.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @param updateGrad  gradient need to update.
     */
    protected abstract void updateGradient(Tuple3<Double, Double, Vector> labelVector,
                                           DenseVector coefVector,
                                           DenseVector updateGrad);

    /**
     * Update hessian matrix by one sample.
     *
     * @param labelVector   a sample of train data.
     * @param coefVector    coefficient of current time.
     * @param updateHessian hessian matrix need to update.
     */
    protected abstract void updateHessian(Tuple3<Double, Double, Vector> labelVector,
                                          DenseVector coefVector,
                                          DenseMatrix updateHessian);

    /**
     * Has second order derivative or not.
     *
     * @return has(true) or not(false).
     */
    public abstract boolean hasSecondDerivative();

    /**
     * Calculate object value.
     *
     * @param labelVectors a sample set of train data.
     * @param coefVector   coefficient of current time.
     * @return Tuple2: objectValue, weightSum.
     */
    public Tuple2<Double, Double> calcObjValue(
        Iterable<Tuple3<Double, Double, Vector>> labelVectors, DenseVector coefVector) {
        double weightSum = 0.0;
        double fVal = 0.0;
        double loss;
        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            loss = calcLoss(labelVector, coefVector);
            fVal += loss * labelVector.f0;
            weightSum += labelVector.f0;
        }
        if (0.0 != weightSum) {
            fVal /= weightSum;
        }
        if (0.0 != l1) {
            fVal += l1 * coefVector.normL1();
        }
        if (0.0 != l2) {
            fVal += l2 * MatVecOp.dot(coefVector, coefVector);
        }
        return new Tuple2<>(fVal, weightSum);
    }

    /**
     * Calculate gradient by a set of samples.
     *
     * @param labelVectors train data.
     * @param coefVector   coefficient of current time.
     * @param grad         gradient.
     * @return weight sum
     */
    public double calcGradient(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                               DenseVector coefVector, DenseVector grad) {
        double weightSum = 0.0;
        for (int i = 0; i < grad.size(); i++) {
            grad.set(i, 0.0);
        }
        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            if (labelVector.f2 instanceof SparseVector) {
               ((SparseVector)(labelVector.f2)).setSize(coefVector.size());
            }
            weightSum += labelVector.f0;
            updateGradient(labelVector, coefVector, grad);
        }
        if (weightSum > 0.0) {
            grad.scaleEqual(1.0 / weightSum);
        }
        if (0.0 != this.l2) {
            grad.plusScaleEqual(coefVector, this.l2 * 2);
        }
        if (0.0 != this.l1) {
            double[] coefArray = coefVector.getData();
            for (int i = 0; i < coefVector.size(); i++) {
                grad.add(i, Math.signum(coefArray[i]) * this.l1);
            }
        }
        return weightSum;
    }

    /**
     * Calculate hessian matrix, gradient and loss by a set of samples.
     *
     * @param labelVectors train data.
     * @param coefVector   coefficient of current time.
     * @param hessian      hessian matrix.
     * @param grad         gradient.
     * @return Tuple2 : weightSum and loss
     */
    public Tuple2<Double, Double> calcHessianGradientLoss(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                                                          DenseVector coefVector,
                                                          DenseMatrix hessian,
                                                          DenseVector grad) {
        if (this.hasSecondDerivative()) {
            int size = grad.size();

            for (int i = 0; i < size; ++i) {
                grad.set(i, 0.0);
                for (int j = 0; j < size; ++j) {
                    hessian.set(i, j, 0.0);
                }
            }
            double weightSum = 0.0;
            double loss = 0.0;
            for (Tuple3<Double, Double, Vector> labledVector : labelVectors) {
                updateHessian(labledVector, coefVector, hessian);
                weightSum += labledVector.f0;
                updateGradient(labledVector, coefVector, grad);
                loss += calcLoss(labledVector, coefVector);
            }
            if (0.0 != this.l1) {
                double tmpVal = this.l1 * weightSum;
                double[] coefArray = coefVector.getData();
                for (int i = 0; i < coefVector.size(); i++) {
                    grad.add(i, Math.signum(coefArray[i]) * tmpVal);
                }
            }
            if (0.0 != this.l2) {
                double tmpVal = this.l2 * 2 * weightSum;
                grad.plusScaleEqual(coefVector, tmpVal);
                for (int i = 0; i < hessian.numRows(); ++i) {
                    hessian.add(i, i, tmpVal);
                }
            }
            return Tuple2.of(weightSum, loss);
        } else {
            throw new UnsupportedOperationException(
                "loss function can't support second derivative, newton precondition can not work.");
        }
    }

    /**
     * Calculate loss values for line search in optimization.
     *
     * @param labelVectors train data.
     * @param coefVector   coefficient of current time.
     * @param dirVec       descend direction of optimization problem.
     * @param beta         step length of line search.
     * @param numStep      num of line search step.
     * @return double[] losses.
     */
    public double[] calcSearchValues(Iterable<Tuple3<Double, Double, Vector>> labelVectors, DenseVector coefVector,
                                     DenseVector dirVec, double beta, int numStep) {
        double[] losses = new double[numStep + 1];

        DenseVector[] stepVec = new DenseVector[numStep + 1];
        stepVec[0] = coefVector.clone();
        DenseVector vecDelta = dirVec.scale(beta);
        for (int i = 1; i < numStep + 1; i++) {
            stepVec[i] = stepVec[i - 1].minus(vecDelta);
        }
        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            for (int i = 0; i < numStep + 1; ++i) {
                losses[i] += calcLoss(labelVector, stepVec[i]) * labelVector.f0;
            }
        }
        return losses;
    }

    /**
     * This function calc losses with constraint, which will be used by owlqn.
     *
     * @param labelVectors train data.
     * @param coefVector   coefficient of current time.
     * @param dirVec       descend direction of optimization problem.
     * @param beta         step length of line search.
     * @param numStep      num of line search step.
     * @return double[] losses.
     */
    public double[] constraintCalcSearchValues(
        Iterable<Tuple3<Double, Double, Vector>> labelVectors,
        DenseVector coefVector, DenseVector dirVec, double beta, int numStep) {
        double[] losses = new double[numStep + 1];
        double[] coefArray = coefVector.getData();
        double[] dirArray = dirVec.getData();
        int size = coefArray.length;
        DenseVector newCoef = new DenseVector(size);
        double[] newCoefArray = newCoef.getData();
        for (int i = 0; i < numStep + 1; ++i) {
            double tmpVal = beta * i;
            for (int s = 0; s < size; ++s) {
                double val = coefArray[s] - tmpVal * dirArray[s];
                if (val * coefArray[s] < 0) {
                    val = 0.0;
                }
                newCoefArray[s] = val;
            }
            for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
                losses[i] += calcLoss(labelVector, newCoef) * labelVector.f0;
            }
        }
        return losses;
    }
}