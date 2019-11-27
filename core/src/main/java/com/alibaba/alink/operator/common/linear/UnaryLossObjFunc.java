package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Unary loss object function.
 */
public class UnaryLossObjFunc extends OptimObjFunc {
    private UnaryLossFunc unaryLossFunc;

    /**
     * Constructor.
     *
     * @param unaryLossFunc loss function.
     * @param params        input parameters.
     */
    public UnaryLossObjFunc(UnaryLossFunc unaryLossFunc, Params params) {
        super(params);
        this.unaryLossFunc = unaryLossFunc;
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
     * Calculate loss of one sample.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @return the loss value and weight value.
     */
    @Override
    protected double calcLoss(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector) {
        double eta = getEta(labelVector, coefVector);
        return this.unaryLossFunc.loss(eta, labelVector.f1);
    }

    /**
     * Update gradient by one sample.
     *
     * @param labelVector a sample of train data.
     * @param coefVector  coefficient of current time.
     * @param updateGrad  gradient need to update.
     */
    @Override
    protected void updateGradient(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                  DenseVector updateGrad) {
        double eta = getEta(labelVector, coefVector);
        double div = labelVector.f0 * unaryLossFunc.derivative(eta, labelVector.f1);
        updateGrad.plusScaleEqual(labelVector.f2, div);
    }

    /**
     * Update hessian matrix by one sample.
     *
     * @param labelVector   a sample of train data.
     * @param coefVector    coefficient of current time.
     * @param updateHessian hessian matrix need to update.
     */
    @Override
    protected void updateHessian(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector,
                                 DenseMatrix updateHessian) {
        Vector vec = labelVector.f2;
        double eta = getEta(labelVector, coefVector);
        double deriv = this.unaryLossFunc.secondDerivative(eta, labelVector.f1) * labelVector.f0;
        if (vec instanceof DenseVector) {
            int nCols = updateHessian.numCols();
            int nRows = updateHessian.numRows();
            double[] data = updateHessian.getData();
            double[] vecData = ((DenseVector)vec).getData();
            int pos = 0;
            for (int j = 0; j < nCols; j++) {
                for (int i = 0; i < nRows; i++) {
                    data[pos++] += vecData[i] * vecData[j] * deriv;
                }
            }

        } else if (vec instanceof SparseVector) {
            double[] data = updateHessian.getData();
            int nRows = updateHessian.numRows();
            int[] indices = ((SparseVector)vec).getIndices();
            double[] values = ((SparseVector)vec).getValues();
            for (int i = 0; i < values.length; i++) {
                for (int j = 0; j < values.length; j++) {
                    data[indices[i] + indices[j] * nRows] += values[i] * values[j] * deriv;
                }
            }
        } else {
            throw new UnsupportedOperationException("not support sparse Hessian matrix computing.");
        }
    }

    private double getEta(Tuple3<Double, Double, Vector> labelVector, DenseVector coefVector) {
        return MatVecOp.dot(labelVector.f2, coefVector);
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
    public double[] calcSearchValues(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                                     DenseVector coefVector,
                                     DenseVector dirVec,
                                     double beta,
                                     int numStep) {
        double[] vec = new double[numStep + 1];
        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            double weight = labelVector.f0;
            double etaCoef = getEta(labelVector, coefVector);
            double etaDelta = getEta(labelVector, dirVec) * beta;
            for (int i = 0; i < numStep + 1; ++i) {
                vec[i] += weight * this.unaryLossFunc.loss(etaCoef - i * etaDelta, labelVector.f1);
            }
        }
        return vec;
    }
}