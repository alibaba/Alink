package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * LayerModel for {@link FuntionalLayer}.
 */
public class FuntionalLayerModel extends LayerModel {
    private FuntionalLayer layer;

    public FuntionalLayerModel(FuntionalLayer layer) {
        this.layer = layer;
    }

    @Override
    public void resetModel(DenseVector weights, int offset) {
    }

    @Override
    public void eval(DenseMatrix data, DenseMatrix output) {
        for (int i = 0; i < data.numRows(); i++) {
            for (int j = 0; j < data.numCols(); j++) {
                output.set(i, j, this.layer.activationFunction.eval(data.get(i, j)));
            }
        }
    }

    @Override
    public void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta) {
        for (int i = 0; i < delta.numRows(); i++) {
            for (int j = 0; j < delta.numCols(); j++) {
                double y = output.get(i, j);
                prevDelta.set(i, j, this.layer.activationFunction.derivative(y) * delta.get(i, j));
            }
        }
    }

    @Override
    public void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset) {
    }
}
