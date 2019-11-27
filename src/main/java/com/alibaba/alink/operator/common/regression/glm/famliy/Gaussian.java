package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.operator.common.regression.glm.link.Identity;

import java.io.Serializable;

/**
 * Gaussian family.
 */
public class Gaussian extends Family implements Serializable {

    /**
     * constructor.
     */
    public Gaussian() {
        this.setDefaultLink(new Identity());
    }

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "gaussian";
    }

    /**
     * @param y:      value.
     * @param weight: weight value.
     * @return init value.
     */
    @Override
    public double initialize(double y, double weight) {
        return y;
    }

    /**
     *
     * @param mu: mean
     * @return variance.
     */
    @Override
    public double variance(double mu) {
        return 1.0;
    }

    /**
     *
     * @param y: y .
     * @param mu: mean.
     * @param weight: weight value.
     * @return deviance.
     */
    @Override
    public double deviance(double y, double mu, double weight) {
        return weight * (y - mu) * (y - mu);
    }

    /**
     *
     * @param mu: mean
     * @return project value/
     */
    @Override
    public double project(double mu) {
        if (mu >= Double.POSITIVE_INFINITY) {
            return Double.MAX_VALUE;
        } else if (mu <= Double.NEGATIVE_INFINITY) {
            return Double.MIN_VALUE;
        } else {
            return mu;
        }
    }

}
