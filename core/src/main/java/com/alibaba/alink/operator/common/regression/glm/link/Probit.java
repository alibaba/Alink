package com.alibaba.alink.operator.common.regression.glm.link;

import com.alibaba.alink.common.utils.AlinkSerializable;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.io.Serializable;

/**
 * Probit link function.
 */
public class Probit extends LinkFunction implements Serializable, AlinkSerializable {

	private static final long serialVersionUID = -6390192204333720480L;

	/**
	 * @return link function name.
	 */
	@Override
	public String name() {
		return "Probit";
	}

	/**
	 * @param mu: mean
	 *            return get eta
	 */
	@Override
	public double link(double mu) {
		NormalDistribution distribution = new NormalDistribution();
		return distribution.inverseCumulativeProbability(mu);
	}

	/**
	 * @param mu: mean
	 * @return deta/dmu
	 */
	@Override
	public double derivative(double mu) {
		NormalDistribution distribution = new NormalDistribution();
		return 1.0 / distribution.density(distribution.inverseCumulativeProbability(mu));
	}

	/**
	 * @param eta: eta
	 * @return getmu
	 */
	@Override
	public double unlink(double eta) {
		NormalDistribution distribution = new NormalDistribution();
		return distribution.cumulativeProbability(eta);
	}

}
