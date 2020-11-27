package com.alibaba.alink.operator.common.regression.glm;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.regression.glm.famliy.Binomial;
import com.alibaba.alink.operator.common.regression.glm.famliy.FamilyFunction;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gamma;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gaussian;
import com.alibaba.alink.operator.common.regression.glm.famliy.Poisson;
import com.alibaba.alink.operator.common.regression.glm.famliy.Tweedie;
import com.alibaba.alink.operator.common.regression.glm.link.CLogLog;
import com.alibaba.alink.operator.common.regression.glm.link.Identity;
import com.alibaba.alink.operator.common.regression.glm.link.Inverse;
import com.alibaba.alink.operator.common.regression.glm.link.LinkFunction;
import com.alibaba.alink.operator.common.regression.glm.link.Log;
import com.alibaba.alink.operator.common.regression.glm.link.Logit;
import com.alibaba.alink.operator.common.regression.glm.link.Power;
import com.alibaba.alink.operator.common.regression.glm.link.Probit;
import com.alibaba.alink.operator.common.regression.glm.link.Sqrt;
import com.alibaba.alink.params.regression.GlmTrainParams;

import java.io.Serializable;

/**
 * Family Link.
 */
public class FamilyLink implements Serializable, AlinkSerializable {
	private static final long serialVersionUID = -5636173906677739363L;

	private FamilyFunction family;
	private LinkFunction link;

	/**
	 * @param family:        family name.
	 * @param variancePower: variance power.
	 * @param link:          link name.
	 * @param linkPower:     link power.
	 */
	public FamilyLink(GlmTrainParams.Family family, double variancePower, GlmTrainParams.Link link, double linkPower) {
		if (family == null) {
			throw new RuntimeException("family can not be empty");
		}

		switch (family) {
			case Gamma:
				this.family = new Gamma();
				break;
			case Binomial:
				this.family = new Binomial();
				break;
			case Gaussian:
				this.family = new Gaussian();
				break;
			case Poisson:
				this.family = new Poisson();
				break;
			case Tweedie:
				this.family = new Tweedie(variancePower);
				break;
		}

		if (link == null) {
			this.link = this.family.getDefaultLink();
		} else {
			switch (link) {
				case CLogLog:
					this.link = new CLogLog();
					break;
				case Identity:
					this.link = new Identity();
					break;
				case Inverse:
					this.link = new Inverse();
					break;
				case Log:
					this.link = new Log();
					break;
				case Logit:
					this.link = new Logit();
					break;
				case Power:
					this.link = new Power(linkPower);
					break;
				case Probit:
					this.link = new Probit();
					break;
				case Sqrt:
					this.link = new Sqrt();
					break;
			}
		}
	}

	/**
	 * @return family.
	 */
	public FamilyFunction getFamily() {
		return family;
	}

	/**
	 * @return link function.
	 */
	public LinkFunction getLink() {
		return link;
	}

	/**
	 * @return family name.
	 */
	String getFamilyName() {
		return family.name();
	}

	/**
	 * @return link name.
	 */
	String getLinkName() {
		return link.name();
	}

	/**
	 * @param mu: mean
	 * @return eta
	 */
	public double predict(double mu) {
		return link.link(family.project(mu));
	}

	/**
	 * @param eta: y
	 * @return mu
	 */
	public double fitted(double eta) {
		return family.project(link.unlink(eta));
	}

	/**
	 * @param coefficients: coefficient of features.
	 * @param intercept:    intercept.
	 * @param features:     features.
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
