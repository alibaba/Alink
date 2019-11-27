package com.alibaba.alink.operator.common.regression.glm.link;

import java.io.Serializable;

/**
 * Identity link function.
 */
public class Identity extends Link implements Serializable {

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "identity";
    }

    /**
     * @param mu: mean
     * return get eta
     */
    @Override
    public double link(double mu) {
        return mu;
    }

    /**
     *
     * @param mu: mean
     * @return deta/dmu
     */
    @Override
    public double derivative(double mu) {
        return 1.0;
    }

    /**
     *
     * @param eta: eta
     * @return getmu
     */
    @Override
    public double unlink(double eta) {
        return eta;
    }

}
