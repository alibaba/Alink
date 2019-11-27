package com.alibaba.alink.operator.common.classification.ann;

/**
 * Defines a sigmoid layer with squared error.
 */
public class SigmoidLayerWithSquaredError extends Layer {
    @Override
    public LayerModel createModel() {
        return new SigmoidLayerModelWithSquaredError();
    }

    @Override
    public int getWeightSize() {
        return 0;
    }

    @Override
    public int getOutputSize(int inputSize) {
        return inputSize;
    }

    @Override
    public boolean isInPlace() {
        return true;
    }
}
