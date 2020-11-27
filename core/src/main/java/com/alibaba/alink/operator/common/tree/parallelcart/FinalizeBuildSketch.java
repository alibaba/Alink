package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.FeatureMeta;

import java.util.Collections;
import java.util.Comparator;

public final class FinalizeBuildSketch extends ComputeFunction {
	private static final long serialVersionUID = 4705452198935672337L;

	@Override
	public void calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (boostingObjs.inWeakLearner) {
			return;
		}

		// rewrite feature meta.
		EpsilonApproQuantile.WQSummary[] summaries = context.getObj(BuildLocalSketch.SKETCH);

		int continuous = 0;

		for (int i = 0; i < boostingObjs.data.getN(); ++i) {
			if (boostingObjs.data.getFeatureMetas()[i].getType()
				.equals(FeatureMeta.FeatureType.CONTINUOUS)) {

				EpsilonApproQuantile.WQSummary summary = summaries[continuous];

				int zeroIndex = Collections.binarySearch(
					summary.entries,
					new EpsilonApproQuantile.Entry(-1, -1, -1, 0.0),
					Comparator.comparingDouble(o -> o.value)
				);

				zeroIndex = zeroIndex < 0 ? -zeroIndex - 1 : zeroIndex;

				boostingObjs.data.getFeatureMetas()[i].rewrite(
					summary.entries.size(),
					zeroIndex,
					summary.entries.size()
				);

				continuous++;
			}
		}
	}
}
