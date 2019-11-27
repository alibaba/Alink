package com.alibaba.alink.operator.common.classification.ann;

/**
 * The sigmoid function. f(x) = 1 / (1 + exp(-x)).
 */
public class SigmoidFunction implements ActivationFunction {
    @Override
    public double eval(double x) {
        return 1.0 / (1 + Math.exp(-x));
    }

    @Override
    public double derivative(double z) {
        return (1 - z) * z;
    }
}
