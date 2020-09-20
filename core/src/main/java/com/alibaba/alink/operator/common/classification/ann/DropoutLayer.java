package com.alibaba.alink.operator.common.classification.ann;

/**
 * Layer properties of dropout layer.
 */
public class DropoutLayer extends Layer {
    public double dropoutRate;

    public DropoutLayer(double dropoutRate) {
        this.dropoutRate = dropoutRate;
    }

    @Override
    public LayerModel createModel() {
        return new DropoutLayerModel(this);
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