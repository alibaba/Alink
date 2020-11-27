package com.alibaba.alink.operator.common.regression.glm.link;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

/**
 * Log link function.
 */
public class Log extends LinkFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = 1405501815471554151L;

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "log";
	}

	/**
	 * @param mu: mean
	 *            return get eta
	 */
	@Override
	public double link(double mu) {
		return Math.log(mu);
	}

	/**
	 * @param mu: mean
	 * @return deta/dmu
	 */
	@Override
	public double derivative(double mu) {
		return 1.0 / mu;
	}

	/**
	 * @param eta: eta
	 * @return getmu
	 */
	@Override
	public double unlink(double eta) {
		return Math.exp(eta);
	}

}
