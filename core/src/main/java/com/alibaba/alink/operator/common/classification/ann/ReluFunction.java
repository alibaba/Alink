package com.alibaba.alink.operator.common.classification.ann;

/**
 * The Relu function.
 * f(x) = max(0, x)
 * f'(x) = 1 if x > 0 else 0
 */
public class ReluFunction implements ActivationFunction {
    @Override
    public double eval(double x) {
        return Math.max(0, x);
    }

    @Override
    public double derivative(double x) {
        if (x > 0) {
            return 1.0;
        } else {
            return 0.0;
        }
    }
}