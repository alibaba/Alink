package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.operator.common.regression.glm.link.Link;

import java.io.Serializable;

/**
 * Family Abstract.
 */
abstract public class Family implements Serializable {

    /**
     * default link function.
     */
    private Link defaultLink;

    /**
     * @param y: value.
     * @param weight: weight value.
     * @return init value.
     */
    public abstract double initialize(double y, double weight);

    /**
     *
     * @param mu: mean
     * @return variance.
     */
    public abstract double variance(double mu);


    /**
     *
     * @param y: y .
     * @param mu: mean.
     * @param weight: weight value.
     * @return deviance.
     */
    public abstract double deviance(double y, double mu, double weight);

    /**
     *
     * @param mu: mean
     * @return project value/
     */
    public double project(double mu) {
        return mu;
    }

    /**
     *
     * @return link function name.
     */
    public abstract String name();

    /**
     *
     * @return default link.
     */
    public Link getDefaultLink() {
        return defaultLink;
    }

    /**
     * @param defaultLink: default link function.
     */
    public void setDefaultLink(Link defaultLink) {
        this.defaultLink = defaultLink;
    }
}
