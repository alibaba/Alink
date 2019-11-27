package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.MatVecOp;

/**
 * The LayerModel for {@link SigmoidLayerModelWithSquaredError}.
 */
public class SigmoidLayerModelWithSquaredError extends FuntionalLayerModel
    implements AnnLossFunction {

    public SigmoidLayerModelWithSquaredError() {
        super(new FuntionalLayer(new SigmoidFunction()));
    }

    @Override
    public double loss(DenseMatrix output, DenseMatrix target, DenseMatrix delta) {
        int batchSize = output.numRows();
        double error = 0.5 * (1. / batchSize) * MatVecOp.applySum(output, target, (x, y) -> (x - y) * (x - y));
        delta.setSubMatrix(output, 0, output.numRows(), 0, output.numCols());
        delta.minusEquals(target);
        MatVecOp.apply(delta, output, delta, (x, o) -> x * o * (1. - o));
        return error;
    }
}
