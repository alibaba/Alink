package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

public class DropoutLayerModel extends LayerModel {
    private DropoutLayer layer;

    public DropoutLayerModel(DropoutLayer layer) {
        this.layer = layer;
    }

    @Override
    public void resetModel(DenseVector weights, int offset) {
    }

    @Override
    public void eval(DenseMatrix data, DenseMatrix output) {
        double dropoutRate = layer.dropoutRate;

        RandomGenerator randomGenerator = new Well19937c(1L);
        BinomialDistribution bionimialDistribution = new BinomialDistribution(randomGenerator,1, 1 - dropoutRate);

        for (int i = 0; i < data.numRows(); i++) {
            for (int j = 0; j < data.numCols(); j++) {
                output.set(i, j, data.get(i, j) * bionimialDistribution.sample() * (1.0 / (1 - dropoutRate)));
            }
        }
    }

    @Override
    public void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta) {
        for (int i = 0; i < delta.numRows(); i++) {
            for (int j = 0; j < delta.numCols(); j++) {
                double y = output.get(i, j);
                prevDelta.set(i, j, y * delta.get(i, j));
            }
        }
    }

    @Override
    public void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset) {

    }
}