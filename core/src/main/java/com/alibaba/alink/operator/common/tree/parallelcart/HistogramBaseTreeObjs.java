package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.data.DataUtil;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.params.classification.GbdtTrainParams;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp.USE_MISSING;
import static com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp.USE_ONEHOT;

final class HistogramBaseTreeObjs {
	Deque <NodeInfoPair> queue = new ArrayDeque <>();
	Integer[] compareIndex4Categorical;
	List <NodeInfoPair.NodeInfo> leaves = new ArrayList <>();
	List <Node> roots = new ArrayList <>();
	int[] featureBinOffset;
	FeatureMeta[] featureMetas;
	int maxFeatureBins;
	int allFeatureBins;
	boolean useMissing;
	boolean useOneHot;
	int[] nodeIdCache;
	QuantileDiscretizerModelDataConverter quantileModel;
	EpsilonApproQuantile.WQSummary[] summaries;
	int maxNodeSize;
	Bagging.BaggingFeaturePool baggingFeaturePool;

	void initPerTree(BoostingObjs boostingObjs, EpsilonApproQuantile.WQSummary[] summaries) {
		assert queue.isEmpty();
		Node root = new Node();
		queue.push(
			new NodeInfoPair(
				new NodeInfoPair.NodeInfo(
					root,
					new Slice(0, boostingObjs.numBaggingInstances),
					new Slice(boostingObjs.numBaggingInstances, boostingObjs.data.getM()),
					1,
					null
				),
				null
			)
		);

		roots.add(root);
		leaves.clear();

		this.summaries = summaries;
	}

	void init(BoostingObjs boostingObjs, List <Row> quantileData) {
		if (quantileData != null && !quantileData.isEmpty()) {
			quantileModel = new QuantileDiscretizerModelDataConverter().load(quantileData);
		}

		useMissing = boostingObjs.params.get(USE_MISSING);
		useOneHot = boostingObjs.params.get(USE_ONEHOT);

		featureMetas = boostingObjs.data.getFeatureMetas();

		maxFeatureBins = 0;
		allFeatureBins = 0;
		for (FeatureMeta featureMeta : boostingObjs.data.getFeatureMetas()) {
			int featureSize = DataUtil.getFeatureCategoricalSize(featureMeta, useMissing);
			maxFeatureBins = Math.max(
				maxFeatureBins,
				featureSize
			);

			allFeatureBins += featureSize;
		}

		compareIndex4Categorical = new Integer[maxFeatureBins];

		featureBinOffset = new int[boostingObjs.numBaggingFeatures];
		nodeIdCache = new int[boostingObjs.data.getM()];

		maxNodeSize = Math.min(
			((int) Math.pow(2, boostingObjs.params.get(GbdtTrainParams.MAX_DEPTH) - 1)),
			boostingObjs.params.get(GbdtTrainParams.MAX_LEAVES)
		);

		baggingFeaturePool = new Bagging.BaggingFeaturePool(maxNodeSize, boostingObjs.numBaggingFeatures);
	}

	void replaceWithActual(Node node) {
		if (node.isLeaf() || node.getCategoricalSplit() != null || useOneHot) {
			return;
		}

		if (summaries != null) {
			node.setContinuousSplit(
				getDynamicSummary(node).entries.get((int) node.getContinuousSplit()).value
			);
		} else {
			node.setContinuousSplit(
				quantileModel
					.getFeatureValue(
						featureMetas[node.getFeatureIndex()].getName(),
						(int) node.getContinuousSplit()
					)
			);
		}
	}

	EpsilonApproQuantile.WQSummary getDynamicSummary(Node node) {
		if (summaries == null) {
			return null;
		}

		for (int i = 0, index = 0; i < featureMetas.length; ++i) {
			if (featureMetas[i].getType().equals(FeatureMeta.FeatureType.CONTINUOUS)) {
				if (featureMetas[i].getIndex() == node.getFeatureIndex()) {
					return summaries[index];
				}

				index++;
			}
		}

		return null;
	}
}
