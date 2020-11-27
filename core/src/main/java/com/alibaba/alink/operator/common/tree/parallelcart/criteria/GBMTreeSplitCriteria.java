package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

import com.alibaba.alink.operator.common.tree.Criteria;

public abstract class GBMTreeSplitCriteria extends Criteria.RegressionCriteria {
	private static final long serialVersionUID = -647112771476117315L;

	public GBMTreeSplitCriteria(double weightSum, int numInstances) {
		super(weightSum, numInstances);
	}

	public abstract double actualGain(Criteria... children);
}
