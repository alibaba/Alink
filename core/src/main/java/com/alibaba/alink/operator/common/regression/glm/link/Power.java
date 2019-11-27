package com.alibaba.alink.operator.common.regression.glm.link;

import java.io.Serializable;

/**
 * Power link function.
 */
public class Power extends Link implements Serializable {
    private double linkPower;
    private double linkPower1;
    private double linkPowerT;

    /**
     * constructor.
     * @param linkPower: power
     */
    public Power(double linkPower) {
        this.linkPower = linkPower;
        if(linkPower != 0.0) {
            this.linkPower1 = linkPower - 1.0;
            this.linkPowerT = 1.0 / linkPower;
        }
    }

    /**
     *
     * @return link function name.
     */
    @Override
    public String name() {
        return "power";
    }

    /**
     *
     * @param mu: mean
     * return get eta
     */
    @Override
    public double link(double mu) {
        if (linkPower == 0.0) {
            return Math.log(mu);
        } else {
            return Math.pow(mu, linkPower);
        }
    }

    /**
     *
     * @param mu: mean
     * @return deta/dmu
     */
    @Override
    public double derivative(double mu) {
        if (linkPower == 0.0) {
            return 1.0 / mu;
        } else {
            return linkPower * Math.pow(mu, linkPower1);
        }
    }

    /**
     *
     * @param eta: eta
     * @return getmu
     */
    @Override
    public double unlink(double eta) {
        if (linkPower == 0.0) {
            return Math.exp(eta);
        } else {
            return Math.pow(eta, linkPowerT);
        }
    }

}
