package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.operator.common.regression.glm.link.Log;

import java.io.Serializable;

/**
 * Poisson family.
 */
public class Poisson extends Family implements Serializable {

    /**
     * constructor.
     */
    public Poisson() {
        this.setDefaultLink(new Log());
    }

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "poisson";
    }

    /**
     * @param y:      value.
     * @param weight: weight value.
     * @return init value.
     */
    @Override
    public double initialize(double y, double weight) {
        if (y < 0) {
            throw new RuntimeException("y must be larger or equal with 0 when poisson.");
        }
        return Math.max(y, GlmUtil.DELTA);
    }

    /**
     *
     * @param mu: mean
     * @return variance.
     */
    @Override
    public double variance(double mu) {
        return mu;
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
        return 2.0 * weight * (y * Math.log(y / mu) - (y - mu));
    }

}
