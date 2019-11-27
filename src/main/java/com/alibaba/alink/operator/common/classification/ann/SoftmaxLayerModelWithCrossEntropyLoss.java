package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;

/**
 * The LayerModel for {@link SoftmaxLayerModelWithCrossEntropyLoss}.
 */
public class SoftmaxLayerModelWithCrossEntropyLoss extends LayerModel
    implements AnnLossFunction {

    @Override
    public double loss(DenseMatrix output, DenseMatrix target, DenseMatrix delta) {
        int batchSize = output.numRows();
        MatVecOp.apply(output, target, delta, (o, t) -> t * Math.log(o));
        double loss = -(1.0 / batchSize) * delta.sum();
        MatVecOp.apply(output, target, delta, (o, t) -> o - t);
        return loss;
    }

    @Override
    public void resetModel(DenseVector weights, int offset) {
    }

    @Override
    public void eval(DenseMatrix data, DenseMatrix output) {
        int batchSize = data.numRows();
        for (int ibatch = 0; ibatch < batchSize; ibatch++) {
            double max = -Double.MAX_VALUE;
            for (int i = 0; i < data.numCols(); i++) {
                double v = data.get(ibatch, i);
                if (v > max) {
                    max = v;
                }
            }
            double sum = 0.;
            for (int i = 0; i < data.numCols(); i++) {
                double res = Math.exp(data.get(ibatch, i) - max);
                output.set(ibatch, i, res);
                sum += res;
            }
            for (int i = 0; i < data.numCols(); i++) {
                double v = output.get(ibatch, i);
                output.set(ibatch, i, v / sum);
            }
        }
    }

    @Override
    public void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta) {
        throw new RuntimeException("SoftmaxLayerModelWithCrossEntropyLoss should be the last layer.");
    }

    @Override
    public void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset) {
    }
}
