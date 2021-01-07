package com.alibaba.alink.operator.common.regression.glm.link;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

/**
 * Sqrt link function.
 */
public class Sqrt extends LinkFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = -8670134540451701168L;

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "Sqrt";
	}

	/**
	 * @param mu: mean
	 *            return get eta
	 */
	@Override
	public double link(double mu) {
		return Math.sqrt(mu);
	}

	/**
	 * @param mu: mean
	 * @return deta/dmu
	 */
	@Override
	public double derivative(double mu) {
		return 1.0 / (2.0 * Math.sqrt(mu));
	}

	/**
	 * @param eta: eta
	 * @return getmu
	 */
	@Override
	public double unlink(double eta) {
		return eta * eta;
	}

}
