package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * The LayerModel for {@link AffineLayer}.
 */
public class AffineLayerModel extends LayerModel {
    private DenseMatrix w;
    private DenseVector b;

    // buffer for holding gradw and gradb
    private DenseMatrix gradw;
    private DenseVector gradb;

    private transient DenseVector ones = null;

    public AffineLayerModel(AffineLayer layer) {
        this.w = new DenseMatrix(layer.numIn, layer.numOut);
        this.b = new DenseVector(layer.numOut);
        this.gradw = new DenseMatrix(layer.numIn, layer.numOut);
        this.gradb = new DenseVector(layer.numOut);
    }

    private void unpack(DenseVector weights, int offset, DenseMatrix w, DenseVector b) {
        int pos = 0;
        for (int i = 0; i < this.w.numRows(); i++) {
            for (int j = 0; j < this.w.numCols(); j++) {
                w.set(i, j, weights.get(offset + pos));
                pos++;
            }
        }
        for (int i = 0; i < this.b.size(); i++) {
            b.set(i, weights.get(offset + pos));
            pos++;
        }
    }

    private void pack(DenseVector weights, int offset, DenseMatrix w, DenseVector b) {
        int pos = 0;
        for (int i = 0; i < this.w.numRows(); i++) {
            for (int j = 0; j < this.w.numCols(); j++) {
                weights.set(offset + pos, w.get(i, j));
                pos++;
            }
        }
        for (int i = 0; i < this.b.size(); i++) {
            weights.set(offset + pos, b.get(i));
            pos++;
        }
    }

    @Override
    public void resetModel(DenseVector weights, int offset) {
        unpack(weights, offset, this.w, this.b);
    }

    @Override
    public void eval(DenseMatrix data, DenseMatrix output) {
        int batchSize = data.numRows();
        for (int i = 0; i < batchSize; i++) {
            for (int j = 0; j < this.b.size(); j++) {
                output.set(i, j, this.b.get(j));
            }
        }
        BLAS.gemm(1., data, false, this.w, false, 1., output);
    }

    @Override
    public void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta) {
        BLAS.gemm(1.0, delta, false, this.w, true, 0., prevDelta);
    }

    @Override
    public void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset) {
        unpack(cumGrad, offset, this.gradw, this.gradb);
        int batchSize = input.numRows();
        BLAS.gemm(1.0, input, true, delta, false, 1.0, this.gradw);
        if (ones == null || ones.size() != batchSize) {
            ones = DenseVector.ones(batchSize);
        }
        BLAS.gemv(1.0, delta, true, this.ones, 1.0, this.gradb);
        pack(cumGrad, offset, this.gradw, this.gradb);
    }
}
