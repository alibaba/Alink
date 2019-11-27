package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Logistic loss function.
 * https://en.wikipedia.org/wiki/Loss_functions_for_classification#Logistic_loss
 */
public class LogisticLossFunc implements UnaryLossFunc {

    private static final double ln2 = Math.log(2.0);

    public LogisticLossFunc() { }

    @Override
    public double loss(double eta, double y) {
        double d = eta * y;
        if (d < -37) {
            return -d / ln2;
        } else if (d > 34) {
            return 0;
        }
        return Math.log(1 + Math.exp(-d)) / ln2;
    }

    @Override
    public double derivative(double eta, double y) {
        double d = eta * y;
        if (d < -37) {
            return -y / ln2;
        } else {
            return -y / (Math.exp(d) + 1) / ln2;
        }
    }

    @Override
    public double secondDerivative(double eta, double y) {
        double t = y / (1 + Math.exp(eta * y));
        return t * (y - t) / ln2;
    }
}
