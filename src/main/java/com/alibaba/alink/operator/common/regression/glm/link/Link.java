package com.alibaba.alink.operator.common.regression.glm.link;

import java.io.Serializable;

/**
 * link abstract.
 */
public abstract class Link implements Serializable {

    /**
     *
     * @param mu: mean
     * return get eta
     */
    public abstract double link(double mu);


    /**
     *
     * @param mu: mean
     * @return deta/dmu
     */
    public abstract double derivative(double mu);


    /**
     *
     * @param eta: eta
     * @return getmu
     */
    public abstract double unlink(double eta);

    /**
     *
     * @return link function name.
     */
    public abstract String name();
}
