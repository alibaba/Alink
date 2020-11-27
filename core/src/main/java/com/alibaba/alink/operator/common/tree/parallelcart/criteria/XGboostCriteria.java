package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.LabelCounter;

public class XGboostCriteria extends HessionBaseCriteria {
	private static final long serialVersionUID = 7247297923336280375L;
	double gradientSum;
	double hessionSum;
	final double lambda;
	final double gamma;

	public XGboostCriteria(
		double lambda,
		double gamma,
		double weightSum,
		int numInstances,
		double gradientSum,
		double hessionSum) {
		super(weightSum, numInstances);
		this.lambda = lambda;
		this.gamma = gamma;
		this.gradientSum = gradientSum;
		this.hessionSum = hessionSum;
	}

	@Override
	public LabelCounter toLabelCounter() {
		return new LabelCounter(weightSum, numInstances, new double[] {gradientSum, hessionSum});
	}

	@Override
	public double impurity() {
		if (hessionSum + lambda == 0) {
			return 0.;
		}

		return gradientSum * gradientSum / (hessionSum + lambda);
	}

	@Override
	public double gain(Criteria... children) {
		double sumChildren = 0.0;

		for (Criteria child : children) {
			sumChildren += child.impurity();
		}

		return 0.5 * (sumChildren - impurity()) - gamma;
	}

	@Override
	public XGboostCriteria add(Criteria other) {
		XGboostCriteria xGboostCriteria = (XGboostCriteria) other;
		this.gradientSum += xGboostCriteria.gradientSum;
		this.hessionSum += xGboostCriteria.hessionSum;
		this.weightSum += xGboostCriteria.weightSum;
		this.numInstances += xGboostCriteria.numInstances;
		return this;
	}

	@Override
	public XGboostCriteria subtract(Criteria other) {
		XGboostCriteria xGboostCriteria = (XGboostCriteria) other;
		this.gradientSum -= xGboostCriteria.gradientSum;
		this.hessionSum -= xGboostCriteria.hessionSum;
		this.weightSum -= xGboostCriteria.weightSum;
		this.numInstances -= xGboostCriteria.numInstances;
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
		return gain(children);
	}
}
