package com.alibaba.alink.operator.common.regression.glm.link;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

/**
 * Identity link function.
 */
public class Identity extends LinkFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = 8936857485582909362L;

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "Identity";
	}

	/**
	 * @param mu: mean
	 *            return get eta
	 */
	@Override
	public double link(double mu) {
		return mu;
	}

	/**
	 * @param mu: mean
	 * @return deta/dmu
	 */
	@Override
	public double derivative(double mu) {
		return 1.0;
	}

	/**
	 * @param eta: eta
	 * @return getmu
	 */
	@Override
	public double unlink(double eta) {
		return eta;
	}

}
