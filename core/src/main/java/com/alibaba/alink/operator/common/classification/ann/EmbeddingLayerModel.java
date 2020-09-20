
package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * The LayerModel for {@link EmbeddingLayer}
 */
public class EmbeddingLayerModel extends LayerModel {
    private DenseMatrix w;

    // buffer for holding gradw
    private DenseMatrix gradw;

    public EmbeddingLayerModel(EmbeddingLayer layer) {
        this.w = new DenseMatrix(layer.numIn, layer.embeddingSize);
        this.gradw = new DenseMatrix(layer.numIn, layer.embeddingSize);
    }

    private void pack(DenseVector weights, int offset, DenseMatrix w) {
        int pos = 0;
        for (int i = 0; i < this.w.numRows(); i++) {
            for (int j = 0; j < this.w.numCols(); j++) {
                weights.set(offset + pos, w.get(i, j));
                pos++;
            }
        }
    }

    private void unpack(DenseVector weights, int offset, DenseMatrix w) {
        int pos = 0;
        for (int i = 0; i < this.w.numRows(); i++) {
            for (int j = 0; j < this.w.numCols(); j++) {
                w.set(i, j, weights.get(offset + pos));
                pos++;
            }
        }
    }

    @Override
    public void resetModel(DenseVector weights, int offset) {
        unpack(weights, offset, this.w);
    }

    @Override
    public void eval(DenseMatrix data, DenseMatrix output) {
        BLAS.gemm(1., data, false, this.w, false, 0., output);
    }

    @Override
    public void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta) {
        BLAS.gemm(1.0, delta, false, this.w, true, 0., prevDelta);
    }

    @Override
    public void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset) {
        unpack(cumGrad, offset, this.gradw);
        int batchSize = input.numRows();
        BLAS.gemm(1.0, input, true, delta, false, 1.0, this.gradw);
        pack(cumGrad, offset, this.gradw);
    }
}