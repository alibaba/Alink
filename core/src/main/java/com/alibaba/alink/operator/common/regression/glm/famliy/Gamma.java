package com.alibaba.alink.operator.common.regression.glm.famliy;

import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.regression.glm.link.Inverse;

import java.io.Serializable;

/**
 * Gamma family.
 */
public class Gamma extends FamilyFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = -4928389185867657173L;

	/**
	 * constructor.
	 */
	public Gamma() {
		this.setDefaultLink(new Inverse());
	}

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "Gamma";
	}

	/**
	 * @param y:      value.
	 * @param weight: weight value.
	 * @return init value.
	 */
	@Override
	public double initialize(double y, double weight) {
		if (y <= 0) {
			throw new RuntimeException("y of Gamma family must be positive.");
		}
		return y;
	}

	/**
	 * @param mu: mean
	 * @return variance.
	 */
	@Override
	public double variance(double mu) {
		return mu * mu;
	}

	/**
	 * @param y:      y .
	 * @param mu:     mean.
	 * @param weight: weight value.
	 * @return deviance.
	 */
	@Override
	public double deviance(double y, double mu, double weight) {
		return -2.0 * weight * (Math.log(y / mu) - (y - mu) / mu);
	}
}
