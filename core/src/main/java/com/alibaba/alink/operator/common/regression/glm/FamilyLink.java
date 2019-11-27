package com.alibaba.alink.operator.common.regression.glm;

import com.alibaba.alink.operator.common.regression.glm.famliy.Binomial;
import com.alibaba.alink.operator.common.regression.glm.famliy.Family;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gamma;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gaussian;
import com.alibaba.alink.operator.common.regression.glm.famliy.Poisson;
import com.alibaba.alink.operator.common.regression.glm.famliy.Tweedie;
import com.alibaba.alink.operator.common.regression.glm.link.CLogLog;
import com.alibaba.alink.operator.common.regression.glm.link.Identity;
import com.alibaba.alink.operator.common.regression.glm.link.Inverse;
import com.alibaba.alink.operator.common.regression.glm.link.Link;
import com.alibaba.alink.operator.common.regression.glm.link.Log;
import com.alibaba.alink.operator.common.regression.glm.link.Logit;
import com.alibaba.alink.operator.common.regression.glm.link.Power;
import com.alibaba.alink.operator.common.regression.glm.link.Probit;
import com.alibaba.alink.operator.common.regression.glm.link.Sqrt;

import java.io.Serializable;

/**
 * Family Link.
 */
public class FamilyLink implements Serializable {
    private Family family;
    private Link link;

    /**
     *
     * @param familyName: family name.
     * @param variancePower: variance power.
     * @param linkName: link name.
     * @param linkPower: link power.
     */
    public FamilyLink(String familyName, double variancePower, String linkName, double linkPower) {
        if (familyName == null || familyName.isEmpty()) {
            throw new RuntimeException("family can not be empty");
        }

        switch (familyName.toLowerCase()) {
            case "gamma":
                family = new Gamma();
                break;
            case "binomial":
                family = new Binomial();
                break;
            case "gaussian":
                family = new Gaussian();
                break;
            case "poisson":
                family = new Poisson();
                break;
            case "tweedie":
                family = new Tweedie(variancePower);
                break;
            default:
                throw new RuntimeException("family is not support. ");
        }

        if (linkName == null || linkName.isEmpty()) {
            link = family.getDefaultLink();
        } else {
            switch (linkName.toLowerCase()) {
                case "cloglog":
                    link = new CLogLog();
                    break;
                case "identity":
                    link = new Identity();
                    break;
                case "inverse":
                    link = new Inverse();
                    break;
                case "log":
                    link = new Log();
                    break;
                case "logit":
                    link = new Logit();
                    break;
                case "power":
                    link = new Power(linkPower);
                    break;
                case "probit":
                    link = new Probit();
                    break;
                case "sqrt":
                    link = new Sqrt();
                    break;
                default:
                    throw new RuntimeException("family is not support. ");
            }
        }
    }

    /**
     * @return family.
     */
    public Family getFamily() {
        return family;
    }

    /**
     *
     * @return link function.
     */
    public Link getLink() {
        return link;
    }

    /**
     *
     * @return family name.
     */
    String getFamilyName() {
        return family.name();
    }

    /**
     *
     * @return link name.
     */
    String getLinkName() {
        return link.name();
    }

    /**
     *
     * @param mu: mean
     * @return eta
     */
    public double predict(double mu) {
        return link.link(family.project(mu));
    }

    /**
     *
     * @param eta: y
     * @return mu
     */
    public double fitted(double eta) {
        return family.project(link.unlink(eta));
    }

    /**
     *
     * @param coefficients: coefficient of features.
     * @param intercept: intercept.
     * @param features: features.
     * @return new weight and label.
     */
    double[] calcWeightAndLabel(double[] coefficients, double intercept, double[] features) {
        int numFeature = coefficients.length;

        double label = features[numFeature];
        double weight = features[numFeature + 1];
        double offset = features[numFeature + 2];

        double eta = GlmUtil.linearPredict(coefficients, intercept, features) + offset;
        double mu = fitted(eta);
        double newLabel = eta - offset + (label - mu) * link.derivative(mu);
        double newWeight = weight / (Math.pow(link.derivative(mu), 2.0) * family.variance(mu));

        features[numFeature] = newLabel;
        features[numFeature + 1] = newWeight;

        return features;
    }
}
