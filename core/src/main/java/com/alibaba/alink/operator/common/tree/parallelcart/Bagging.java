package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.operator.common.tree.paralleltree.TreeObj;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class Bagging extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(Bagging.class);
	private static final long serialVersionUID = -4306518497657978646L;
	private int[] queryBaggingIndices;
	private int queryBaggingCnt;

	@Override
	public void calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (boostingObjs.inWeakLearner) {
			return;
		}

		LOG.info("taskId: {}, {} start", context.getTaskId(), Bagging.class.getSimpleName());
		if (LossUtils.isRanking(boostingObjs.params.get(LossUtils.LOSS_TYPE))) {
			int size = boostingObjs.data.getQueryIdOffset().length - 1;
			if (context.getStepNo() == 1) {
				queryBaggingIndices = new int[size];

				for (int i = 0; i < size; ++i) {
					queryBaggingIndices[i] = i;
				}

				queryBaggingCnt = (int) Math.min(
					size,
					Math.ceil(
						size * boostingObjs.params.get(GbdtTrainParams.SUBSAMPLING_RATIO)
					)
				);
			}

			TreeObj.shuffle(queryBaggingIndices, boostingObjs.instanceRandomizer);

			int[] queryIdOffset = boostingObjs.data.getQueryIdOffset();
			int numBaggingInstances = 0;
			for (int i = 0; i < queryBaggingCnt; ++i) {
				int queryIdOffsetIndex = queryBaggingIndices[i];
				int start = queryIdOffset[queryIdOffsetIndex];
				int end = queryIdOffset[queryIdOffsetIndex + 1];
				for (int j = start; j < end; ++j) {
					boostingObjs.indices[numBaggingInstances++] = j;
				}
			}

			boostingObjs.numBaggingInstances = numBaggingInstances;
		} else {
			TreeObj.shuffle(boostingObjs.indices, boostingObjs.instanceRandomizer);
		}

		LOG.info("taskId: {}, {} end", context.getTaskId(), Bagging.class.getSimpleName());
	}

	public static int[] sampleFeatures(
		BoostingObjs boostingObjs,
		BaggingFeaturePool pool) {

		int[] features = pool.get();
		TreeObj.shuffle(boostingObjs.featureIndices, boostingObjs.featureRandomizer);
		System.arraycopy(boostingObjs.featureIndices, 0, features, 0, boostingObjs.numBaggingFeatures);
		Arrays.sort(features);

		return features;
	}

	public static final class BaggingFeaturePool {
		private final int[][] pool;
		private int cursor;

		public BaggingFeaturePool(int maxNodeSize, int numBaggingFeature) {
			pool = new int[maxNodeSize][numBaggingFeature];
		}

		public int[] get() {
			return pool[cursor++];
		}

		public void reset() {
			cursor = 0;
		}
	}
}
