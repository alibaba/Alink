package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.io.Serializable;
import java.util.List;

/**
 * The TopologyModel object holds weights of a neural network topology, and defines methods
 * to compute output, gradient and delta.
 *
 * @see {@link Topology}.
 */
public abstract class TopologyModel implements Serializable {
    /**
     * Reset the model with new weights.
     *
     * @param weights The weights.
     */
    public abstract void resetModel(DenseVector weights);

    /**
     * Forward the input through all layers.
     *
     * @param data             The input data.
     * @param includeLastLayer Whether to include last layer.
     * @return A list of outputs, one for each layer.
     */
    public abstract List<DenseMatrix> forward(DenseMatrix data, boolean includeLastLayer);

    /**
     * Compute the prediction.
     *
     * @param features Input features.
     * @return The logits.
     */
    public abstract DenseVector predict(DenseVector features);

    /**
     * Compute gradients of all layers and the loss value.
     *
     * @param data    The input data.
     * @param target  The target value.
     * @param cumGrad The gradient.
     * @return The loss value.
     */
    public abstract double computeGradient(DenseMatrix data, DenseMatrix target, DenseVector cumGrad);
}
