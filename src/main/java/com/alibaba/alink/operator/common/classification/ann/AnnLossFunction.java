package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;

/**
 * The interface for ANN loss function.
 */
public interface AnnLossFunction {
    /**
     * Compute the loss and writes delta inplace.
     *
     * @param y      The output of last layer.
     * @param yTruth The ground truth value.
     * @param delta  The delta for back-propagation.
     * @return The loss value.
     */
    double loss(DenseMatrix y, DenseMatrix yTruth, DenseMatrix delta);
}
