package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.parallelcart.booster.Booster;
import com.alibaba.alink.operator.common.tree.parallelcart.data.DataUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.regression.GbdtRegTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.Future;

public class ConstructLocalHistogram extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(ConstructLocalHistogram.class);
	private static final long serialVersionUID = -325487480296758683L;
	private static final int STEP = 4;

	private BitSet featureValid;
	private int[] aligned;
	private int[] validFeatureOffset;
	private double[] featureSplitHistogram;
	private Future <?>[] results;
	private boolean useInstanceCount;

	private final DefaultDistributedInfo distributedInfo = new DefaultDistributedInfo();

	@Override
	public void calc(ComContext context) {
		LOG.info("taskId: {}, {} start", context.getTaskId(), ConstructLocalHistogram.class.getSimpleName());
		BoostingObjs boostingObjs = context.getObj("boostingObjs");
		Booster booster = context.getObj("booster");
		HistogramBaseTreeObjs tree = context.getObj("tree");

		if (context.getStepNo() == 1) {

			LOG.info("maxDepth: {}, maxLeaves: {}",
				boostingObjs.params.get(GbdtRegTrainParams.MAX_DEPTH),
				boostingObjs.params.get(GbdtRegTrainParams.MAX_LEAVES)
			);

			int histogramLen = Math.min(
				tree.maxNodeSize * tree.maxFeatureBins * STEP * boostingObjs.numBaggingFeatures,
				tree.maxNodeSize * STEP * tree.allFeatureBins
			);

			int featureSplitHistogramLen = tree.maxNodeSize * STEP * tree.allFeatureBins;

			context.putObj("histogram", new double[histogramLen]);
			context.putObj("recvcnts", new int[context.getNumTask()]);

			featureSplitHistogram = new double[featureSplitHistogramLen];
			featureValid = new BitSet(boostingObjs.data.getN());
			aligned = new int[boostingObjs.data.getM()];
			validFeatureOffset = new int[boostingObjs.data.getN()];
			results = new Future[boostingObjs.data.getN()];

			useInstanceCount = LossUtils.useInstanceCount(boostingObjs.params.get(LossUtils.LOSS_TYPE));
		}

		if (!boostingObjs.inWeakLearner) {
			tree.initPerTree(boostingObjs, context.getObj(BuildLocalSketch.SKETCH));
			boostingObjs.inWeakLearner = true;
		}

		double[] histogram = context.getObj("histogram");

		int sumFeatureCount = calcWithNodeIdCache(context, boostingObjs, booster, tree, histogram);

		int[] recvcnts = context.getObj("recvcnts");

		Arrays.fill(recvcnts, 0);

		// split the histogram by feature
		int taskPos = 0;
		int featureCnt = 0;
		int next = (int) (
			distributedInfo.startPos(taskPos, context.getNumTask(), sumFeatureCount)
				+ distributedInfo.localRowCnt(taskPos, context.getNumTask(), sumFeatureCount)
		);

		for (NodeInfoPair item : tree.queue) {
			final int[] smallBaggingFeatures = item.small.baggingFeatures;

			for (int smallBaggingFeature : smallBaggingFeatures) {
				featureCnt++;

				while (featureCnt > next) {
					taskPos++;
					next = (int) (
						distributedInfo.startPos(taskPos, context.getNumTask(), sumFeatureCount)
							+ distributedInfo.localRowCnt(taskPos, context.getNumTask(), sumFeatureCount)
					);
				}

				recvcnts[taskPos] += DataUtil.getFeatureCategoricalSize(
					boostingObjs.data.getFeatureMetas()[smallBaggingFeature],
					tree.useMissing) * STEP;
			}

			if (item.big != null) {
				final int[] bigBaggingFeatures = item.big.baggingFeatures;

				for (int bigBaggingFeature : bigBaggingFeatures) {
					featureCnt++;

					while (featureCnt > next) {
						taskPos++;
						next = (int) (
							distributedInfo.startPos(taskPos, context.getNumTask(), sumFeatureCount)
								+ distributedInfo.localRowCnt(taskPos, context.getNumTask(), sumFeatureCount)
						);
					}

					recvcnts[taskPos] += DataUtil.getFeatureCategoricalSize(
						boostingObjs.data.getFeatureMetas()[bigBaggingFeature],
						tree.useMissing) * STEP;
				}
			}
		}

		LOG.info("taskId: {}, {} end", context.getTaskId(), ConstructLocalHistogram.class.getSimpleName());
	}

	public int calcWithNodeIdCache(
		ComContext context,
		BoostingObjs boostingObjs,
		Booster booster,
		HistogramBaseTreeObjs tree,
		double[] histogram) {

		int nodeSize = 0;
		int sumFeatureCount = 0;

		featureValid.clear();
		Arrays.fill(tree.nodeIdCache, -1);
		tree.baggingFeaturePool.reset();

		for (NodeInfoPair item : tree.queue) {
			item.small.baggingFeatures = Bagging.sampleFeatures(boostingObjs, tree.baggingFeaturePool);

			for (int i = 0; i < item.small.baggingFeatures.length; ++i) {
				featureValid.set(item.small.baggingFeatures[i], true);
			}

			for (int i = item.small.slice.start; i < item.small.slice.end; ++i) {
				tree.nodeIdCache[boostingObjs.indices[i]] = nodeSize;
			}

			nodeSize++;
			sumFeatureCount += boostingObjs.numBaggingFeatures;

			if (item.big != null) {
				item.big.baggingFeatures = Bagging.sampleFeatures(boostingObjs, tree.baggingFeaturePool);

				for (int i = 0; i < item.big.baggingFeatures.length; ++i) {
					featureValid.set(item.big.baggingFeatures[i], true);
				}

				for (int i = item.big.slice.start; i < item.big.slice.end; ++i) {
					tree.nodeIdCache[boostingObjs.indices[i]] = nodeSize;
				}

				nodeSize++;
				sumFeatureCount += boostingObjs.numBaggingFeatures;
			}
		}

		if (boostingObjs.params.get(BaseGbdtTrainBatchOp.USE_EPSILON_APPRO_QUANTILE)) {
			EpsilonApproQuantile.WQSummary[] summaries = context.getObj(BuildLocalSketch.SKETCH);

			int sumFeatureSize = 0;
			for (int i = 0; i < boostingObjs.data.getN(); ++i) {
				FeatureMeta featureMeta = boostingObjs.data.getFeatureMetas()[i];

				validFeatureOffset[i] = sumFeatureSize;
				if (featureValid.get(i)) {
					sumFeatureSize +=
						DataUtil.getFeatureCategoricalSize(featureMeta, tree.useMissing);
				}
			}

			boostingObjs.data.constructHistogramWithWQSummary(
				useInstanceCount,
				nodeSize,
				featureValid,
				tree.nodeIdCache,
				validFeatureOffset,
				booster.getGradients(),
				booster.getHessions(),
				booster.getWeights(),
				summaries,
				boostingObjs.executorService,
				results,
				featureSplitHistogram
			);

		} else {
			int sumFeatureSize = 0;
			for (int i = 0; i < boostingObjs.data.getN(); ++i) {
				validFeatureOffset[i] = sumFeatureSize;
				if (featureValid.get(i)) {
					sumFeatureSize += DataUtil.getFeatureCategoricalSize(
						boostingObjs.data.getFeatureMetas()[i],
						tree.useMissing);
				}
			}

			int validInstanceCount = 0;

			for (int i = 0; i < boostingObjs.data.getM(); ++i) {
				if (tree.nodeIdCache[i] < 0) {
					continue;
				}
				aligned[validInstanceCount] = i;
				validInstanceCount++;
			}

			LOG.info(
				"taskId: {}, calcWithNodeIdCache start",
				context.getTaskId()
			);

			boostingObjs.data.constructHistogram(
				useInstanceCount,
				nodeSize,
				validInstanceCount,
				featureValid,
				tree.nodeIdCache,
				validFeatureOffset,
				aligned,
				booster.getGradients(),
				booster.getHessions(),
				booster.getWeights(),
				boostingObjs.executorService,
				results,
				featureSplitHistogram
			);
		}

		LOG.info(
			"taskId: {}, calcWithNodeIdCache end",
			context.getTaskId()
		);

		int histogramOffset = 0;
		int nodeId = 0;
		for (NodeInfoPair item : tree.queue) {
			for (int i = 0; i < item.small.baggingFeatures.length; ++i) {
				int featureSize = DataUtil.getFeatureCategoricalSize(
					boostingObjs.data.getFeatureMetas()[item.small.baggingFeatures[i]],
					tree.useMissing);

				System.arraycopy(
					featureSplitHistogram,
					(validFeatureOffset[item.small.baggingFeatures[i]] * nodeSize + nodeId * featureSize) * STEP,
					histogram, histogramOffset * STEP,
					featureSize * STEP
				);

				histogramOffset += featureSize;
			}

			nodeId++;

			if (item.big != null) {
				for (int i = 0; i < item.big.baggingFeatures.length; ++i) {
					int featureSize = DataUtil.getFeatureCategoricalSize(
						boostingObjs.data.getFeatureMetas()[item.big.baggingFeatures[i]],
						tree.useMissing);
					System.arraycopy(
						featureSplitHistogram,
						(validFeatureOffset[item.big.baggingFeatures[i]] * nodeSize + nodeId * featureSize) * STEP,
						histogram, histogramOffset * STEP,
						featureSize * STEP
					);

					histogramOffset += featureSize;
				}

				nodeId++;
			}
		}

		LOG.info(
			"taskId: {}, sumFeatureCount: {}",
			context.getTaskId(), sumFeatureCount
		);

		return sumFeatureCount;
	}
}
