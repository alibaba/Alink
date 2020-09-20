package com.alibaba.alink.operator.common.classification.ann;

/**
 * The tanh function.
 * tanh(x) = (exp(x) - exp(-x)) / (exp(x) + exp(-x))
 * tanh'(x) = 1 - tanh(z)^2
 */
public class TanhFunction implements ActivationFunction {
    @Override
    public double eval(double x) {
        return (Math.exp(x) - Math.exp(-x)) / (Math.exp(x) + Math.exp(-x));
    }

    @Override
    public double derivative(double z) {
        return 1 - Math.pow(z, 2.0);
    }
}