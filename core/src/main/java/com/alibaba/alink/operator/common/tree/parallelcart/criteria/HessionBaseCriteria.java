package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

public abstract class HessionBaseCriteria extends GBMTreeSplitCriteria {

	private static final long serialVersionUID = -4996694481908088135L;

	public HessionBaseCriteria(double weightSum, int numInstances) {
		super(weightSum, numInstances);
	}

	@Override
	public void add(double labelValue, double weight, int numInstances) {
		throw new UnsupportedOperationException("");
	}

	public abstract void add(double gradientSum, double hessionSum, double weight, int numInstances);

	public abstract void subtract(double gradientSum, double hessionSum, double weight, int numInstances);

	@Override
	public void subtract(double labelValue, double weight, int numInstances) {
		throw new UnsupportedOperationException("");
	}
}
