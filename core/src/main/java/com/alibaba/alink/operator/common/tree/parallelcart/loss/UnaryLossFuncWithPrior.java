package com.alibaba.alink.operator.common.tree.parallelcart.loss;

import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;

public interface UnaryLossFuncWithPrior extends UnaryLossFunc {
	double prior();
}
