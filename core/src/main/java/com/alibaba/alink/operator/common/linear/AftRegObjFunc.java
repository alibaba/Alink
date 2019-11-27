package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Accelerated failure time Regression object function.
 */
public class AftRegObjFunc extends OptimObjFunc {

    /**
     * Constructor.
     *
     * @param params input parameters.
     */
    public AftRegObjFunc(Params params) {
        super(params);
    }

    /**
     * Calculate dot value of sample vector and coefficient vector.
     * When predicting the result, we only use beta, so the coefVector length may be longer.
     * The length of the two input vectors is not necessarily equal！！
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return the dot of the two input vectors.
     */
    public static double getDotProduct(Vector labelVector, DenseVector coefVector) {
        double[] data = coefVector.getData();
        double sum = 0.0;
        if (labelVector instanceof SparseVector) {
            int[] indices = ((SparseVector)labelVector).getIndices();
            double[] values = ((SparseVector)labelVector).getValues();
            for (int i = 0; i < indices.length; ++i) {
                sum += values[i] * data[indices[i]];
            }
        } else {
            double[] vecData = ((DenseVector)labelVector).getData();
            for (int i = 0; i < vecData.length; i++) {
                sum += vecData[i] * data[i];
            }
        }
        return sum;
    }

    /**
     * Calculate loss of one sample.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return the loss value and weight.
     */
    @Override
    protected double calcLoss(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector) {
        /**
         * loss = censor * coefVector.get(coefVector.size() - 1) - censor * epsilon + Math.exp(epsilon)
         * the last one of coefVector is the log(sigma)
         */
        double epsilon = (labelVector.f1 - getDotProduct(labelVector.f2, coefVector)) /
            Math.exp(coefVector.get(coefVector.size() - 1));
        return labelVector.f0 * (coefVector.get(coefVector.size() - 1) - epsilon) + Math.exp(epsilon);
    }

    /**
     * Update the gradient with a sample of train data.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @param updateGrad  gradient need to update.
     */
    @Override
    protected void updateGradient(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                  DenseVector updateGrad) {

        double sigma = Math.exp(coefVector.get(coefVector.size() - 1));
        double epsilon = (labelVector.f1 - getDotProduct(labelVector.f2, coefVector)) / sigma;
        double multiplier = labelVector.f0 - Math.exp(epsilon);
        double coefficient = multiplier / sigma;

        if (labelVector.f2 instanceof SparseVector) {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int i = 0; i < indices.length; ++i) {
                updateGrad.add(indices[i], values[i] * coefficient);
            }
        } else {
            double[] vecData = ((DenseVector)labelVector.f2).getData();
            for (int i = 0; i < vecData.length; ++i) {
                updateGrad.add(i, vecData[i] * coefficient);
            }
        }
        //the last one is log(sigma), and it's the grad of log(sigma).
        updateGrad.add(coefVector.size() - 1, labelVector.f0 + multiplier * epsilon);
    }

    /**
     * Update the Hessian matrix with a sample of train data.
     *
     * @param labelVector   a sample of train data.
     * @param coefVector    coefficient of current time.
     * @param updateHessian hessian matrix need to update.
     */
    @Override
    protected void updateHessian(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                 DenseMatrix updateHessian) {
        double sigma = Math.exp(coefVector.get(coefVector.size() - 1));
        double epsilon = (labelVector.f1 - getDotProduct(labelVector.f2, coefVector)) / sigma;
        double multiplier = Math.exp(epsilon) / (sigma * sigma);
        if (labelVector.f2 instanceof SparseVector) {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int i = 0; i < indices.length; ++i) {
                double tempVal = values[i] * multiplier;
                for (int j = 0; j < i; ++j) {
                    double temp = tempVal * values[j];
                    updateHessian.add(indices[i], indices[j], temp);
                    updateHessian.add(indices[j], indices[i], temp);
                }
                updateHessian.add(indices[i], indices[i], tempVal * values[i]);
            }
        } else {
            double[] vecData = ((DenseVector)labelVector.f2).getData();
            for (int i = 0; i < vecData.length; ++i) {
                double tempVal = vecData[i] * multiplier;
                for (int j = 0; j < i; ++j) {
                    double temp = tempVal * vecData[j];
                    updateHessian.add(i, j, temp);
                    updateHessian.add(j, i, temp);
                }
                updateHessian.add(i, i, tempVal * vecData[i]);
            }
        }

        updateHessian.add(coefVector.size() - 1, coefVector.size() - 1,
                epsilon * (Math.exp(epsilon) * (1 + epsilon) - labelVector.f0));
    }

    /**
     * Has second order derivative or not.
     *
     * @return has(true) or not(false).
     */
    @Override
    public boolean hasSecondDerivative() {
        return true;
    }

    /**
     * Calculate object value.
     *
     * @param labelVectors a sample set of train data.
     * @param coefVector   coefficient of current time.
     * @return Tuple2: objectValue, weightSum.
     */
    @Override
    public Tuple2<Double, Double> calcObjValue(
        Iterable<Tuple3<Double, Double, Vector>> labelVectors, DenseVector coefVector) {
        double weightSum = 0.0;
        double fVal = 0.0;
        double loss;
        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            loss = calcLoss(labelVector, coefVector);
            fVal += loss;
            weightSum += 1.0;
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
            weightSum += 1.0;
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
    @Override
    public Tuple2<Double, Double> calcHessianGradientLoss(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                                                          DenseVector coefVector, DenseMatrix hessian,
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
                weightSum += 1.0;
                updateGradient(labledVector, coefVector, grad);
                loss += calcLoss(labledVector, coefVector);
            }
            if (0.0 != this.l2) {
                double tmpVal = this.l2 * 2 * weightSum;
                grad.plusScaleEqual(coefVector, tmpVal);
                for (int i = 0; i < hessian.numRows(); ++i) {
                    hessian.add(i, i, tmpVal);
                }
            }
            if (0.0 != this.l1) {
                double tmpVal = this.l1 * weightSum;
                double[] coefArray = coefVector.getData();
                for (int i = 0; i < coefVector.size(); i++) {
                    grad.add(i, Math.signum(coefArray[i]) * tmpVal);
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
    @Override
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
                losses[i] += calcLoss(labelVector, stepVec[i]);
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
    @Override
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
                losses[i] += calcLoss(labelVector, newCoef);
            }
        }
        return losses;
    }

}
