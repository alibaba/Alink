package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.common.statistics.DistributionFuncName;

import java.io.Serializable;

public abstract class ValueDist<V> implements Serializable {

	private static final long serialVersionUID = -1599142545153907097L;

	/**
	 * -------------- for random search ----------------
	 */
	public static ValueDistInteger randInteger(int start, int end) {
		return new ValueDistInteger(start, end);
	}

	public static ValueDistLong randLong(long start, long end) {
		return new ValueDistLong(start, end);
	}

	public static <K> ValueDistArray <K> randArray(K[] values) {
		return new ValueDistArray <K>(values);
	}

	/**
	 * -------------- for bayes optim search ----------------
	 */
	public static ValueDistFunc exponential(double lambda) {
		return new ValueDistFunc(DistributionFuncName.Exponential, new double[] {lambda});
	}

	public static ValueDistFunc uniform(double lowerBound, double upperBound) {
		return new ValueDistFunc(DistributionFuncName.Uniform, new double[] {lowerBound, upperBound});
	}

	public static ValueDistFunc normal(double mu, double sigma2) {
		return new ValueDistFunc(DistributionFuncName.Normal, new double[] {mu, sigma2});
	}

	public static ValueDistFunc stdNormal() {
		return new ValueDistFunc(DistributionFuncName.StdNormal, new double[] {});
	}

	public static ValueDistFunc chi2(double df) {
		return new ValueDistFunc(DistributionFuncName.Chi2, new double[] {df});
	}

	public abstract V get(double p);

}
