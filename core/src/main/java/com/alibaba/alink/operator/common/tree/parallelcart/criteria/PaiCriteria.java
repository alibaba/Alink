package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.LabelCounter;

public class PaiCriteria extends HessionBaseCriteria {
	private static final long serialVersionUID = -4251358962194586557L;
	double gradientSum;
	double hessionSum;
	public static final double PAI_EPS = 0.000001;

	public PaiCriteria(double weightSum, int numInstances, double gradientSum, double hessionSum) {
		super(weightSum, numInstances);
		this.gradientSum = gradientSum;
		this.hessionSum = hessionSum;
	}

	@Override
	public LabelCounter toLabelCounter() {
		return new LabelCounter(weightSum, numInstances, new double[] {gradientSum, hessionSum});
	}

	@Override
	public double impurity() {
		if (weightSum == 0) {
			return 0.;
		}

		return gradientSum * gradientSum / weightSum;
	}

	@Override
	public double gain(Criteria... children) {
		double gain = 0.;

		for (Criteria criteria : children) {
			gain += criteria.impurity();
		}

		return gain < PAI_EPS ? INVALID_GAIN : gain;
	}

	@Override
	public PaiCriteria add(Criteria other) {
		PaiCriteria paiGbdtCriteria = (PaiCriteria) other;
		this.gradientSum += paiGbdtCriteria.gradientSum;
		this.hessionSum += paiGbdtCriteria.hessionSum;
		this.weightSum += paiGbdtCriteria.weightSum;
		this.numInstances += paiGbdtCriteria.numInstances;
		return this;
	}

	@Override
	public PaiCriteria subtract(Criteria other) {
		PaiCriteria paiGbdtCriteria = (PaiCriteria) other;
		this.gradientSum -= paiGbdtCriteria.gradientSum;
		this.hessionSum -= paiGbdtCriteria.hessionSum;
		this.weightSum -= paiGbdtCriteria.weightSum;
		this.numInstances -= paiGbdtCriteria.numInstances;
		return this;
	}

	@Override
	public void reset() {
		this.gradientSum = 0.0;
		this.hessionSum = 0.0;
		this.weightSum = 0.0;
		this.numInstances = 0;
	}

	@Override
	public void add(double gradientSum, double hessionSum, double weight, int numInstances) {
		this.gradientSum += gradientSum;
		this.hessionSum += hessionSum;
		this.weightSum += weight;
		this.numInstances += numInstances;
	}

	@Override
	public void subtract(double gradientSum, double hessionSum, double weight, int numInstances) {
		this.gradientSum -= gradientSum;
		this.hessionSum -= hessionSum;
		this.weightSum -= weight;
		this.numInstances -= numInstances;
	}

	@Override
	public double actualGain(Criteria... children) {
		double gain = 0.;

		for (Criteria criteria : children) {
			gain += criteria.impurity();
		}

		gain -= this.impurity();

		return gain < PAI_EPS ? INVALID_GAIN : gain;
	}
}
