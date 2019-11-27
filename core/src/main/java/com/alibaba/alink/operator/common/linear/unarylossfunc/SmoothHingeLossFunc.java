package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Hinge loss function.
 * More http://qwone.com/~jason/writing/smoothHinge.pdf
 */
public class SmoothHingeLossFunc implements UnaryLossFunc {
    public SmoothHingeLossFunc() { }

    @Override
    public double loss(double eta, double y) {

        double d = eta * y;
        if (d <= 0) {
            return 0.5 - d;
        } else if (d >= 1.0) {
            return 0.0;
        } else {
            return 0.5 * (1 - d) * (1 - d);
        }
    }

    @Override
    public double derivative(double eta, double y) {
        double d = eta * y;
        if (d <= 0) {
            return -y;
        } else if (d >= 1.0) {
            return 0;
        } else {
            return (1 - d) * (-y);
        }
    }

    @Override
    public double secondDerivative(double eta, double y) {
        double d = eta * y;
        if (d >= 1.0) {
            return 0.0;
        } else if (d <= 0.0) {
            return 0.0;
        } else {
            return y * y;
        }
    }
}
