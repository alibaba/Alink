package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.data.DataUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CalcFeatureGain extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(CalcFeatureGain.class);
	private static final long serialVersionUID = 3072216204653127272L;
	private HistogramFeatureSplitter[] featureSplitters;

	@Override
	public void calc(ComContext context) {
		LOG.info("taskId: {}, {} start", context.getTaskId(), CalcFeatureGain.class.getSimpleName());
		BoostingObjs boostingObjs = context.getObj("boostingObjs");
		HistogramBaseTreeObjs tree = context.getObj("tree");
		double[] histogram = context.getObj("histogram");

		if (context.getStepNo() == 1) {
			context.putObj("best", new Node[tree.maxNodeSize]);

			featureSplitters = new HistogramFeatureSplitter[boostingObjs.data.getN()];

			for (int i = 0; i < boostingObjs.data.getN(); ++i) {
				featureSplitters[i] = createFeatureSplitter(
					boostingObjs.data.getFeatureMetas()[i].getType() == FeatureMeta.FeatureType.CATEGORICAL,
					boostingObjs.params,
					boostingObjs.data.getFeatureMetas()[i],
					tree.compareIndex4Categorical
				);
			}
		}

		int sumFeatureCount = 0;
		for (NodeInfoPair item : tree.queue) {
			sumFeatureCount += boostingObjs.numBaggingFeatures;

			if (item.big != null) {
				sumFeatureCount += boostingObjs.numBaggingFeatures;
			}
		}

		DistributedInfo distributedInfo = new DefaultDistributedInfo();
		int start = (int) distributedInfo.startPos(context.getTaskId(), context.getNumTask(), sumFeatureCount);
		int cnt = (int) distributedInfo.localRowCnt(context.getTaskId(), context.getNumTask(), sumFeatureCount);
		int end = start + cnt;

		int featureCnt = 0;
		int featureBinCnt = 0;
		Node[] best = context.getObj("best");

		int index = 0;
		for (NodeInfoPair item : tree.queue) {
			best[index] = null;

			final int[] smallBaggingFeatures = item.small.baggingFeatures;
			for (int smallBaggingFeature : smallBaggingFeatures) {
				if (featureCnt >= start && featureCnt < end) {
					featureSplitters[smallBaggingFeature].reset(
						histogram,
						new Slice(
							featureBinCnt,
							featureBinCnt
								+ DataUtil.getFeatureCategoricalSize(
								boostingObjs.data.getFeatureMetas()[smallBaggingFeature],
								tree.useMissing)
						),
						item.small.depth
					);

					double gain = featureSplitters[smallBaggingFeature].bestSplit(tree.leaves.size());

					if (best[index] == null || (featureSplitters[smallBaggingFeature].canSplit() && gain > best[index]
						.getGain())) {
						best[index] = new Node();
						featureSplitters[smallBaggingFeature].fillNode(best[index]);
					}

					featureBinCnt += DataUtil.getFeatureCategoricalSize(
						boostingObjs.data.getFeatureMetas()[smallBaggingFeature],
						tree.useMissing);
				}
				featureCnt++;
			}

			index++;

			if (item.big != null) {
				best[index] = null;

				final int[] bigBaggingFeatures = item.big.baggingFeatures;
				for (int bigBaggingFeature : bigBaggingFeatures) {
					if (featureCnt >= start && featureCnt < end) {

						featureSplitters[bigBaggingFeature].reset(
							histogram,
							new Slice(
								featureBinCnt,
								featureBinCnt + DataUtil.getFeatureCategoricalSize(
									boostingObjs.data.getFeatureMetas()[bigBaggingFeature],
									tree.useMissing)
							),
							item.big.depth
						);

						double gain = featureSplitters[bigBaggingFeature].bestSplit(tree.leaves.size());

						if (best[index] == null || (featureSplitters[bigBaggingFeature].canSplit() && gain >
							best[index]
							.getGain())) {
							best[index] = new Node();
							featureSplitters[bigBaggingFeature].fillNode(best[index]);
						}

						featureBinCnt += DataUtil.getFeatureCategoricalSize(
							boostingObjs.data.getFeatureMetas()[bigBaggingFeature],
							tree.useMissing);
					}
					featureCnt++;
				}

				index++;
			}
		}

		context.putObj("bestLength", index);
		LOG.info("taskId: {}, {} end", context.getTaskId(), CalcFeatureGain.class.getSimpleName());
	}

	private HistogramFeatureSplitter createFeatureSplitter(
		boolean isCategorical,
		Params params,
		FeatureMeta featureMeta,
		Integer[] compareIndex4Categorical) {

		if (isCategorical) {
			return new HistogramCategoricalFeatureSplitter(params, featureMeta, compareIndex4Categorical);
		} else {
			return new HistogramContinuousFeatureSplitter(params, featureMeta);
		}
	}
}
