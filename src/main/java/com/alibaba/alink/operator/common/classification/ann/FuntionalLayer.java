package com.alibaba.alink.operator.common.classification.ann;

/**
 * Functional layer properties, y = f(x). Functional layer does not hold parameters.
 */
public class FuntionalLayer extends Layer {
    public ActivationFunction activationFunction;

    public FuntionalLayer(ActivationFunction activationFunction) {
        this.activationFunction = activationFunction;
    }

    @Override
    public LayerModel createModel() {
        return new FuntionalLayerModel(this);
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
