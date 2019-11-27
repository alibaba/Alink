package com.alibaba.alink.operator.common.regression.glm.link;

import java.io.Serializable;

/**
 * Logit link function.
 */
public class Logit extends Link implements Serializable {

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "logit";
    }

    /**
     *
     * @param mu: mean
     * return get eta
     */
    @Override
    public double link(double mu) {
        return Math.log(mu / (1.0 - mu));
    }

    /**
     *
     * @param mu: mean
     * @return deta/dmu
     */
    @Override
    public double derivative(double mu) {
        return 1.0 / (mu * (1.0 - mu));
    }

    /**
     *
     * @param eta: eta
     * @return getmu
     */
    @Override
    public double unlink(double eta) {
        return 1.0 / (1.0 + Math.exp(-1.0 * eta));
    }
}
