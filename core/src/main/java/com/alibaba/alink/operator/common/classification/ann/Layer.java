package com.alibaba.alink.operator.common.classification.ann;

import java.io.Serializable;

/**
 * The base class that defines the meta data of a neural network layer.
 *
 * @see {@link LayerModel}.
 */
public abstract class Layer implements Serializable {
    /**
     * Create a <code>LayerModel</code>.
     */
    public abstract LayerModel createModel();

    /**
     * Get the size of weights of the layer.
     */
    public abstract int getWeightSize();

    /**
     * Get the size of output.
     */
    public abstract int getOutputSize(int inputSize);

    /**
     * If true, the memory is not allocated for the output of this layer.
     * The memory allocated to the previous layer is used to write the output of this layer.
     */
    public abstract boolean isInPlace();
}
