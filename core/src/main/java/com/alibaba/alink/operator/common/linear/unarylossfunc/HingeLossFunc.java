package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Hinge loss function.
 * More https://en.wikipedia.org/wiki/Loss_functions_for_classification#Hinge_loss
 */
public class HingeLossFunc implements UnaryLossFunc {
    public HingeLossFunc() { }

    @Override
    public double loss(double eta, double y) {
        return Math.max(0, 1 - eta * y);
    }

    @Override
    public double derivative(double eta, double y) {
        return eta * y < 1 ? -y : 0;
    }

    @Override
    public double secondDerivative(double eta, double y) {
        return 0;
    }
}
