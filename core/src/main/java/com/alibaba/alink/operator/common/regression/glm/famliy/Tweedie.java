package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.operator.common.regression.glm.link.Power;

import java.io.Serializable;

/**
 * Tweedie family.
 */
public class Tweedie extends Family implements Serializable {

    public double variancePower;
    public  double variancePower1;
    public double variancePower2;

    /**
     * constructor.
     * @param variancePower: variance power.
     */
    public Tweedie(double variancePower) {
        this.variancePower = variancePower;
        this.setDefaultLink(new Power(1 - variancePower));
        this.variancePower1 = 1 - variancePower;
        this.variancePower2 = 2 - variancePower;
    }

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "tweedie";
    }

    /**
     * @param y: value.
     * @param weight: weight value.
     * @return init value.
     */
    @Override
    public double initialize(double y, double weight) {
        if (y == 0) {
            return GlmUtil.DELTA;
        } else {
            return y;
        }
    }

    /**
     *
     * @param mu: mean
     * @return variance.
     */
    @Override
    public double variance(double mu) {
        return Math.pow(mu, variancePower);
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
        double y1 = y, theta, kappa;

        if (y == 0) {
            y1 = 1;
        }

        if (variancePower == 1) {
            theta = Math.log(y1 / mu);
        } else {
            theta = (Math.pow(y1, variancePower1) - Math.pow(mu, variancePower1)) / variancePower1;
        }

        if (variancePower == 2) {
            kappa = Math.log(y1 / mu);
        } else {
            kappa = (Math.pow(y, variancePower2) - Math.pow(mu, variancePower2)) / variancePower2;
        }

        double deviance = 2 * weight * (y * theta - kappa);
        return deviance < GlmUtil.EPSILON ? GlmUtil.EPSILON : deviance;
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
        } else if (Double.isInfinite(mu)) {
            return Double.MAX_VALUE;
        } else {
            return mu;
        }
    }
}
