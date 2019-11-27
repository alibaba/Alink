package com.alibaba.alink.operator.common.classification.ann;

import java.io.Serializable;

/**
 * The interface for non-linear activation function.
 */
public interface ActivationFunction extends Serializable {
    /**
     * Evaluate at point <code>x</code>.
     */
    double eval(double x);

    /**
     * Compute derivative at point <code>z</code>.
     */
    double derivative(double z);
}
