package com.alibaba.alink.operator.common.classification.ann;

/**
 * Layer properties of embedding transformations which is y = A * x.
 */
public class EmbeddingLayer extends Layer {
    public int numIn;
    public int embeddingSize;

    public EmbeddingLayer(int numIn, int embeddingSize) {
        this.numIn = numIn;
        this.embeddingSize = embeddingSize;
    }

    @Override
    public LayerModel createModel() {
        return new EmbeddingLayerModel(this);
    }

    @Override
    public int getWeightSize() {
        return numIn * embeddingSize;
    }

    @Override
    public int getOutputSize(int inputSize) {
        return embeddingSize;
    }

    @Override
    public boolean isInPlace() {
        return false;
    }
}