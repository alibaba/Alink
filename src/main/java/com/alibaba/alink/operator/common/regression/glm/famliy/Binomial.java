package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.operator.common.regression.glm.link.Logit;

import java.io.Serializable;

/**
 * Binomial family.
 */
public class Binomial extends Family implements Serializable {

    /**
     * constructor.
     */
    public Binomial() {
        this.setDefaultLink(new Logit());
    }

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "binomial";
    }

    /**
     * @param y: value.
     * @param weight: weight value.
     * @return init value.
     */
    @Override
    public double initialize(double y, double weight) {
        double mu = (weight * y + 0.5) / (weight + 1.0);
        if (mu <= 0 || mu >= 1.0) {
            throw new RuntimeException("mu must be in (0, 1).");
        }
        return mu;
    }

    /**
     *
     * @param mu: mean
     * @return variance.
     */
    @Override
    public double variance(double mu) {
        return mu * (1 - mu);
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
        return 2.0 * weight * (ylogy(y, mu) + ylogy(1.0 - y, 1.0 - mu));
    }

    private double ylogy(double y, double mu) {
        return y == 0 ? 0 : y * Math.log(y / mu);
    }


    /**
     *
     * @param mu: mean
     * @return project value/
     */
    @Override
    public double project(double mu) {
        if (mu < GlmUtil.EPSILON) {
            return GlmUtil.EPSILON;
        } else if (mu > 1.0 - GlmUtil.EPSILON) {
            return 1.0 - GlmUtil.EPSILON;
        } else {
            return mu;
        }
    }
}
