package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * The LayerModel object holds weights of a neural network layer, and defines methods
 * to compute layer output, gradient and delta.
 *
 * @see {@link Layer}.
 */
public abstract class LayerModel {
    /**
     * Reset the LayerModel with provided weights and offset.
     *
     * @param weights The weights of the layer.
     * @param offset  Starting position in <code>weights</code>.
     */
    public abstract void resetModel(DenseVector weights, int offset);

    /**
     * Evaluate the output of the layer.
     *
     * @param data   The input data of the layer.
     * @param output The buffer holding the output of the layer.
     */
    public abstract void eval(DenseMatrix data, DenseMatrix output);

    /**
     * Compute the delta for back propagation.
     *
     * @param delta     Delta of current layer.
     * @param output    Output of current layer.
     * @param prevDelta Delta for previous layer.
     */
    public abstract void computePrevDelta(DenseMatrix delta, DenseMatrix output, DenseMatrix prevDelta);

    /**
     * Compute the gradient of the weights of current layer.
     *
     * @param delta   Delta of current layer.
     * @param input   Input of current layer.
     * @param cumGrad The cumulative gradient.
     * @param offset  Offset in <code>cumGrad</code>.
     */
    public abstract void grad(DenseMatrix delta, DenseMatrix input, DenseVector cumGrad, int offset);
}
