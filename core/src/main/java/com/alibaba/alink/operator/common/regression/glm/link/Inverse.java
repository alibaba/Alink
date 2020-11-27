package com.alibaba.alink.operator.common.regression.glm.link;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

/**
 * Inverse link function.
 */
public class Inverse extends LinkFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = 5670008043956229965L;

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "Inverse";
	}

	/**
	 * @param mu: mean
	 *            return get eta
	 */
	@Override
	public double link(double mu) {
		return 1.0 / mu;
	}

	/**
	 * @param mu: mean
	 * @return deta/dmu
	 */
	@Override
	public double derivative(double mu) {
		return -1.0 * Math.pow(mu, -2.0);
	}

	/**
	 * @param eta: eta
	 * @return getmu
	 */
	@Override
	public double unlink(double eta) {
		return 1.0 / eta;
	}

}
