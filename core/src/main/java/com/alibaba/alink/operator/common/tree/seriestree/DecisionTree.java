package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumSubsetFeatures;
import com.alibaba.alink.params.shared.tree.HasSeed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Random;

public class DecisionTree {
	private DenseData data;
	private Params params;
	private Deque<Tuple2<Node, FeatureSplitter[]>> queue = new ArrayDeque<>();
	private Random random;

	public DecisionTree(DenseData data, Params params) {
		this.data = data;
		this.params = params;
		this.random = new Random(params.get(HasSeed.SEED));
	}

	public Node fit() {
		Node root = new Node();

		init(root);

		while (!queue.isEmpty()) {
			Tuple2<Node, FeatureSplitter[]> item = queue.poll();

			FeatureSplitter best = fitNode(bagging(item.f1), queue.size());

			best.fillNode(item.f0);

			if (!best.canSplit) {
				item.f0.makeLeaf();
				item.f0.makeLeafProb();
				continue;
			}

			split(item.f1, item.f0, best);
		}

		return root;
	}

	private FeatureSplitter fitNode(FeatureSplitter[] splitters, int leafNodeCount) {
		double bestGain = Criteria.INVALID_GAIN;
		FeatureSplitter bestSplitter = null;
		for (FeatureSplitter splitter : splitters) {
			double gain = splitter.bestSplit(leafNodeCount);

			if (gain > bestGain || bestSplitter == null) {
				bestGain = gain;
				bestSplitter = splitter;
			}
		}

		return bestSplitter;
	}

	private void split(FeatureSplitter[] all, Node node, FeatureSplitter best) {
		FeatureSplitter[][] childSplitters = best.split(all);
		Node[] nextNodes = new Node[childSplitters.length];

		for (int i = 0; i < childSplitters.length; ++i) {
			nextNodes[i] = new Node();
			queue.add(Tuple2.of(nextNodes[i], childSplitters[i]));
		}

		node.setNextNodes(nextNodes);
	}

	private FeatureSplitter[] bagging(FeatureSplitter[] splitters) {
		shuffle(splitters, random);

		return Arrays.copyOf(
			splitters,
			Math.max(1,
				Math.min(
					(int) (params.get(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO) * splitters.length),
					Math.min(
						splitters.length,
						params.get(HasNumSubsetFeatures.NUM_SUBSET_FEATURES)
					)
				)
			)
		);
	}

	private static <T> void shuffle(T[] array, Random random) {
		for (int i = array.length - 1; i > 0; --i) {
			int idx = random.nextInt(i + 1);

			if (idx == i) {
				continue;
			}

			T tmp = array[idx];
			array[idx] = array[i];
			array[i] = tmp;
		}
	}

	private SequentialPartition initSequentialPartition() {
		List<Tuple2<Integer, Double>> dataIndices = new ArrayList<>(data.weights.length);
		for (int i = 0; i < data.weights.length; ++i) {
			dataIndices.add(Tuple2.of(i, data.weights[i]));
		}

		return new SequentialPartition(dataIndices);
	}

	private FeatureSplitter[] initSplitters(SequentialPartition partition) {
		FeatureSplitter[] splitters = new FeatureSplitter[data.featureMetas.length];
		for (int i = 0; i < data.featureMetas.length; ++i) {
			if (data.featureMetas[i].getType() == FeatureMeta.FeatureType.CATEGORICAL) {
				splitters[i] = new CategoricalSplitter(params, data, data.featureMetas[i], partition);
			} else {
				splitters[i] = new ContinuousSplitter(params, data, data.featureMetas[i], partition);
			}
		}

		return splitters;
	}

	private void init(Node root) {
		queue.push(Tuple2.of(root, initSplitters(initSequentialPartition())));
	}

}
