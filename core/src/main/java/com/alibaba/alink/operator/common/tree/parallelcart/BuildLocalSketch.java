package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.Booster;
import com.alibaba.alink.operator.common.tree.parallelcart.communication.AllReduceT;

public final class BuildLocalSketch extends ComputeFunction {
	public final static String SKETCH = "sketch";
	public final static String FEATURE_SKETCH_LENGTH = "featureSketchLength";
	private static final long serialVersionUID = 6357309989554983054L;

	EpsilonApproQuantile.SketchEntry[] entries;

	@Override
	public void calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (boostingObjs.inWeakLearner) {
			// skip build sketch including all reduce.
			context.putObj(FEATURE_SKETCH_LENGTH, 0);
			return;
		}

		int continuous = 0;

		for (int i = 0; i < boostingObjs.data.getN(); ++i) {
			if (boostingObjs.data.getFeatureMetas()[i].getType()
				.equals(FeatureMeta.FeatureType.CONTINUOUS)) {

				continuous++;
			}
		}

		if (context.getStepNo() == 1) {
			boostingObjs.data.sort();

			EpsilonApproQuantile.WQSummary[] summary = context.getObj(SKETCH);

			if (summary == null) {
				summary = new EpsilonApproQuantile.WQSummary[continuous];

				for (int i = 0; i < continuous; ++i) {
					summary[i] = new EpsilonApproQuantile.WQSummary();
				}

				context.putObj(SKETCH, summary);
			}
		}

		EpsilonApproQuantile.WQSummary[] summary = context.getObj(SKETCH);

		double eps = boostingObjs.params.get(BaseGbdtTrainBatchOp.SKETCH_EPS);
		int maxSize = maxSize(boostingObjs.params.get(BaseGbdtTrainBatchOp.SKETCH_RATIO), eps);
		AkPreconditions.checkState(maxSize > 0);

		if (entries == null) {
			entries = new EpsilonApproQuantile.SketchEntry[continuous];

			for (int i = 0; i < continuous; ++i) {
				EpsilonApproQuantile.SketchEntry entry = new EpsilonApproQuantile.SketchEntry();
				entry.sketch.limitSizeLevel(boostingObjs.data.getM(), eps);
				entries[i] = entry;
			}
		}

		boostingObjs.baggingFlags.clear();

		for (int i = 0; i < boostingObjs.numBaggingInstances; ++i) {
			boostingObjs.baggingFlags.set(boostingObjs.indices[i]);
		}

		boostingObjs.data.createWQSummary(
			maxSize, eps,
			entries,
			context. <Booster>getObj(Boosting.BOOSTER).getHessions(),
			boostingObjs.baggingFlags
		);

		for (int i = 0; i < entries.length; ++i) {
			entries[i].sketch.getSummary(summary[i]);
		}

		context.putObj(FEATURE_SKETCH_LENGTH, continuous);
	}

	public static int maxSize(double ratio, double eps) {
		return (int) (ratio / eps);
	}

	public final static class SketchReducer
		implements
		AllReduceT.SerializableBiConsumer <EpsilonApproQuantile.WQSummary[], EpsilonApproQuantile.WQSummary[]> {
		private static final long serialVersionUID = -5949438933636979107L;
		private final int maxSize;

		public SketchReducer(Params params) {
			this.maxSize = maxSize(
				params.get(BaseGbdtTrainBatchOp.SKETCH_RATIO),
				params.get(BaseGbdtTrainBatchOp.SKETCH_EPS)
			);
		}

		@Override
		public void accept(EpsilonApproQuantile.WQSummary[] summaryLeft,
						   EpsilonApproQuantile.WQSummary[] summaryRight) {
			AkPreconditions.checkState(
				summaryLeft != null && summaryRight != null && summaryLeft.length == summaryRight.length
			);

			for (int i = 0; i < summaryLeft.length; ++i) {
				EpsilonApproQuantile.WQSummary temp = new EpsilonApproQuantile.WQSummary();
				temp.setCombine(summaryLeft[i], summaryRight[i]);
				summaryLeft[i].setPrune(temp, maxSize);
			}
		}
	}
}
