package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import com.alibaba.alink.operator.common.tree.parallelcart.BoostingObjs;

public interface Booster {
	void boosting(BoostingObjs boostingObjs, double[] label, double[] pred);

	double[] getWeights();

	double[] getGradients();

	double[] getHessions();

	double[] getGradientsSqr();
}
