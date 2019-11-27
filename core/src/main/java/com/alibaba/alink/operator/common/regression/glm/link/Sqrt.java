package com.alibaba.alink.operator.common.regression.glm.link;

import java.io.Serializable;

/**
 * Sqrt link function.
 */
public class Sqrt extends Link implements Serializable {

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "sqrt";
    }

    /**
     *
     * @param mu: mean
     * return get eta
     */
    @Override
    public double link(double mu) {
        return Math.sqrt(mu);
    }

    /**
     *
     * @param mu: mean
     * @return deta/dmu
     */
    @Override
    public double derivative(double mu) {
        return 1.0 / (2.0 * Math.sqrt(mu));
    }

    /**
     *
     * @param eta: eta
     * @return getmu
     */
    @Override
    public double unlink(double eta) {
        return eta * eta;
    }

}
