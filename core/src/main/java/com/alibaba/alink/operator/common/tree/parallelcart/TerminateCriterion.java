package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.params.classification.GbdtTrainParams;

public final class TerminateCriterion extends CompareCriterionFunction {
	private static final long serialVersionUID = -2393009304147589520L;

	@Override
	public boolean calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		return !boostingObjs.inWeakLearner
			&& boostingObjs.numBoosting == boostingObjs.params.get(GbdtTrainParams.NUM_TREES);
	}
}
