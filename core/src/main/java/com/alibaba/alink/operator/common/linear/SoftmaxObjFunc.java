package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.Arrays;

/**
 * Softmax object function.
 */
public class SoftmaxObjFunc extends OptimObjFunc {
    /**
     * k1 = numClass - 1
     */
    private final int k1;
    private Tuple2<double[], double[]> etas = null;

    /**
     * Constructor.
     *
     * @param params input parameters.
     */
    public SoftmaxObjFunc(Params params) {
        super(params);
        this.k1 = this.params.get(ModelParamName.NUM_CLASSES) - 1;
    }

    /**
     * Calculate loss of one sample.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return the loss value and weight value.
     */
    @Override
    protected double calcLoss(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector) {
        int featDim = coefVector.size() / k1;
        double[] weights = coefVector.getData();
        double sumExp = 1;
        double eta;
        double loss = 0;
        int yk = labelVector.f1.intValue();
        if (labelVector.f2 instanceof DenseVector) {
            double[] x = ((DenseVector)labelVector.f2).getData();
            for (int k = 0; k < k1; k++) {
                eta = 0;
                for (int i = 0; i < featDim; i++) {
                    eta += x[i] * weights[k * featDim + i];
                }
                if (yk == k) {
                    loss -= eta;
                }
                sumExp += Math.exp(eta);
            }
        } else {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int k = 0; k < k1; k++) {
                eta = 0;
                for (int i = 0; i < indices.length; ++i) {
                    eta += values[i] * weights[k * featDim + indices[i]];
                }
                if (yk == k) {
                    loss -= eta;
                }
                sumExp += Math.exp(eta);
            }
        }
        loss += Math.log(sumExp);
        return loss;
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
    public double[] calcSearchValues(
        Iterable<Tuple3<Double, Double, Vector>> labelVectors, DenseVector coefVector,
        DenseVector dirVec, double beta, int numStep) {
        double[] losses = new double[numStep + 1];
        double[] stateValues = new double[numStep + 1];

        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            if (etas == null) {
                double[] f0 = new double[k1 + 1];
                double[] f1 = new double[k1 + 1];
                etas = Tuple2.of(f0, f1);
            }
            calcEta(labelVector, coefVector, dirVec, beta, etas);
            int yk = labelVector.f1.intValue();

            for (int i = 0; i < numStep + 1; ++i) {
                losses[i] -= (etas.f0[yk] - i * etas.f1[yk]);
            }
            for (int k = 0; k < k1; k++) {
                etas.f0[k] = Math.exp(etas.f0[k]);
                etas.f1[k] = Math.exp(etas.f1[k]);
            }

            Arrays.fill(stateValues, 0, numStep + 1, 1);

            for (int k = 0; k < k1; k++) {
                double t = etas.f0[k];
                for (int i = 0; i <= numStep; i++) {
                    stateValues[i] += t;
                    t /= etas.f1[k];
                }
            }
            for (int i = 0; i < numStep + 1; ++i) {
                losses[i] += Math.log(stateValues[i]) * labelVector.f0;
            }
        }
        return losses;
    }

    /**
     * Calculate middle value eta.
     *
     * @param labelVector train data.
     * @param coefVector  coefficient of current time.
     * @param dirVec      descend direction of optimization problem.
     * @param beta        step length of line search.
     * @param etas        middle value.
     */
    private void calcEta(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                         DenseVector dirVec, double beta, Tuple2<double[], double[]> etas) {
        int m = coefVector.size() / k1;
        double[] weights = coefVector.getData();
        double[] dirVals = dirVec.getData();
        double[] eta = etas.f0;
        double[] delta = etas.f1;
        int idx;
        if (labelVector.f2 instanceof DenseVector) {
            double[] vals = ((DenseVector)labelVector.f2).getData();
            for (int k = 0; k < k1; k++) {
                eta[k] = 0;
                delta[k] = 0;
                for (int i = 0; i < m; i++) {
                    idx = i + k * m;
                    eta[k] += vals[i] * weights[idx];
                    delta[k] += vals[i] * dirVals[idx] * beta;
                }
            }
        } else {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int k = 0; k < k1; k++) {
                eta[k] = 0;
                delta[k] = 0;
                for (int i = 0; i < indices.length; ++i) {
                    idx = indices[i] + k * m;
                    eta[k] += values[i] * weights[idx];
                    delta[k] += values[i] * dirVals[idx] * beta;
                }
            }
        }
    }

    /**
     * Update gradient by one labelVector.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @param updateGrad  gradient need to update.
     * @return weight sum value
     */
    @Override
    protected void updateGradient(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                  DenseVector updateGrad) {
        double[] phi = calcPhi(labelVector, coefVector);

        int yk = labelVector.f1.intValue();
        if (yk < k1) {
            phi[yk] -= 1;
        }

        int featDim = coefVector.size() / k1;

        if (labelVector.f2 instanceof SparseVector) {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            double tmpVal;
            int tmpIdx;
            for (int k = 0; k < k1; k++) {
                tmpVal = phi[k] * labelVector.f0;
                tmpIdx = k * featDim;
                for (int i = 0; i < indices.length; ++i) {
                    updateGrad.add(tmpIdx + indices[i], values[i] * tmpVal);
                }
            }
        } else {
            double[] vals = ((DenseVector)labelVector.f2).getData();
            int tmpIdx;
            double tmpVal;
            for (int k = 0; k < k1; k++) {
                tmpVal = phi[k] * labelVector.f0;
                tmpIdx = k * featDim;
                for (int i = 0; i < featDim; i++) {
                    updateGrad.add(tmpIdx + i, vals[i] * tmpVal);
                }
            }
        }
    }

    /**
     * Calculate middle value phi.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return values of phi(middle values).
     */
    private double[] calcPhi(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector) {
        double[] phi = new double[k1];
        int m = coefVector.size() / k1;
        double[] w = coefVector.getData();
        double expSum = 1;
        double eta;
        double[] x;
        if (labelVector.f2 instanceof DenseVector) {
            x = ((DenseVector)labelVector.f2).getData();
            for (int k = 0; k < k1; k++) {
                eta = 0;
                for (int i = 0; i < m; i++) {
                    eta += x[i] * w[k * m + i];
                }
                phi[k] = Math.exp(eta);
                expSum += phi[k];
            }
        } else {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int k = 0; k < k1; k++) {
                eta = 0;
                for (int i = 0; i < indices.length; ++i) {
                    eta += values[i] * w[k * m + indices[i]];
                }
                phi[k] = Math.exp(eta);
                expSum += phi[k];
            }
        }

        for (int k = 0; k < k1; k++) {
            phi[k] /= expSum;
        }
        return phi;
    }

    /**
     * Update hessian matrix.
     *
     * @param labelVector   a sample of train data.
     * @param coefVector    coefficient of current time.
     * @param updateHessian hessian matrix need to update.
     * @return weight sum value
     */
    @Override
    protected void updateHessian(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                 DenseMatrix updateHessian) {
        double[] phi = calcPhi(labelVector, coefVector);
        int featDim = coefVector.size() / k1;
        if (labelVector.f2 instanceof DenseVector) {
            double[] vals = ((DenseVector)labelVector.f2).getData();
            for (int s = 0; s < k1; s++) {
                double scale = phi[s] - phi[s] * phi[s];
                int offset = s * featDim;
                for (int i = 0; i < featDim; i++) {
                    for (int j = 0; j < featDim; j++) {
                        updateHessian.add(i + offset, j + offset, vals[i] * vals[j] * scale);
                    }
                }
            }
            for (int s = 0; s < k1; s++) {
                for (int t = s + 1; t < k1; t++) {
                    double scale = -phi[s] * phi[t];
                    int offsetI = s * featDim;
                    int offsetJ = t * featDim;
                    for (int i = 0; i < featDim; i++) {
                        for (int j = 0; j < featDim; j++) {
                            double d = vals[i] * vals[j] * scale;
                            updateHessian.add(i + offsetI, j + offsetJ, d);
                            updateHessian.add(j + offsetJ, i + offsetI, d);
                        }
                    }
                }
            }
        } else {
            int[] indices = ((SparseVector)labelVector.f2).getIndices();
            double[] values = ((SparseVector)labelVector.f2).getValues();
            for (int s = 0; s < k1; s++) {
                double scale = phi[s] - phi[s] * phi[s];
                int offset = s * featDim;
                for (int i = 0; i < indices.length; i++) {
                    int tmpIdx = indices[i] + offset;
                    double tmpVal = values[i] * scale;
                    for (int j = 0; j < indices.length; j++) {
                        updateHessian.add(tmpIdx, indices[j] + offset, values[j] * tmpVal);
                    }
                }
            }
            for (int s = 0; s < k1; s++) {
                for (int t = s + 1; t < k1; t++) {
                    double scale = -phi[s] * phi[t];
                    int offsetI = s * featDim;
                    int offsetJ = t * featDim;
                    for (int i = 0; i < indices.length; i++) {
                        for (int j = 0; j < indices.length; j++) {
                            double d = values[i] * values[j] * scale;
                            updateHessian.add(indices[i] + offsetI, indices[j] + offsetJ, d);
                            updateHessian.add(indices[j] + offsetJ, indices[i] + offsetI, d);
                        }
                    }
                }
            }
        }
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
}