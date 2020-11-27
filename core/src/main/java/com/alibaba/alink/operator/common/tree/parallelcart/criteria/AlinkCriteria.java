package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.LabelCounter;

public class AlinkCriteria extends HessionBaseCriteria {
	private static final long serialVersionUID = 3057273388324167712L;
	double gradientSum;
	double hessionSum;

	public AlinkCriteria(double weightSum, int numInstances, double gradientSum, double hessionSum) {
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
		return gradientSum * gradientSum / hessionSum;
	}

	@Override
	public double gain(Criteria... children) {
		if (hessionSum == 0.0) {
			return 0.0;
		}
		double rootImpurity = impurity();

		double childrenImpuritySum = 0.0;
		for (Criteria child : children) {
			AlinkCriteria aChild = (AlinkCriteria) child;
			if (aChild.hessionSum == 0.0) {
				return 0.0;
			}

			childrenImpuritySum += aChild.impurity();
		}

		return Math.abs(childrenImpuritySum - rootImpurity);
	}

	@Override
	public AlinkCriteria add(Criteria other) {
		AlinkCriteria alinkOldCriteria = (AlinkCriteria) other;
		this.gradientSum += alinkOldCriteria.gradientSum;
		this.weightSum += alinkOldCriteria.weightSum;
		this.numInstances += alinkOldCriteria.numInstances;
		return this;
	}

	@Override
	public AlinkCriteria subtract(Criteria other) {
		AlinkCriteria alinkOldCriteria = (AlinkCriteria) other;
		this.gradientSum -= alinkOldCriteria.gradientSum;
		this.weightSum -= alinkOldCriteria.weightSum;
		this.numInstances -= alinkOldCriteria.numInstances;
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
		this.gradientSum += gradientSum * weight;
		this.hessionSum += hessionSum * weight;
		this.weightSum += weight;
		this.numInstances += numInstances;
	}

	@Override
	public void subtract(double gradientSum, double hessionSum, double weight, int numInstances) {
		this.gradientSum -= gradientSum * weight;
		this.hessionSum -= hessionSum * weight;
		this.weightSum -= weight;
		this.numInstances -= numInstances;
	}

	@Override
	public double actualGain(Criteria... children) {
		return gain(children);
	}
}
