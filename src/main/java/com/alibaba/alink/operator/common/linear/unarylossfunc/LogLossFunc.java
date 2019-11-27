package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Log loss function.
 * https://en.wikipedia.org/wiki/Loss_functions_for_classification#Cross_entropy_loss_(Log_Loss)
 */
public class LogLossFunc implements UnaryLossFunc {
    public LogLossFunc() { }

    @Override
    public double loss(double eta, double y) {
        double d = eta * y;
        if (d < -37) {
            return -d;
        } else if (d > 34) {
            return 0;
        }
        return Math.log(1 + Math.exp(-d));
    }

    @Override
    public double derivative(double eta, double y) {
        double d = eta * y;
        if (d < -37) {
            return -y;
        } else if (d > 34) {
            return 0;
        }
        return -y / (Math.exp(d) + 1);
    }

    @Override
    public double secondDerivative(double eta, double y) {
        double t = y / (1 + Math.exp(eta * y));
        return t * (y - t);
    }
}
