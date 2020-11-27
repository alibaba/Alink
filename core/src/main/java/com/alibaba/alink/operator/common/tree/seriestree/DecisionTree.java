package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.tree.HasSeed;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DecisionTree {
	private DenseData data;
	private Params params;
	private Deque <Tuple2 <Node, SequentialFeatureSplitter[]>> queue = new ArrayDeque <>();
	private Random random;

	private static final int NUM_THREADS = 4;
	private final SequentialPartition[] threadLocals;
	private final ExecutorService executorService;
	private final ArrayList <Future <Double>> futures;

	public DecisionTree(DenseData data, Params params, ExecutorService executorService) {
		this.data = data;
		this.params = params;
		this.random = new Random(params.get(HasSeed.SEED));

		Preconditions.checkNotNull(executorService);

		this.executorService = executorService;

		threadLocals = new SequentialPartition[NUM_THREADS];
		futures = new ArrayList <>(NUM_THREADS);

		for (int i = 0; i < NUM_THREADS; ++i) {
			threadLocals[i] = new SequentialPartition(new ArrayList <>());
		}
	}

	public Node fit() {
		Node root = new Node();

		init(root);

		while (!queue.isEmpty()) {
			Tuple2 <Node, SequentialFeatureSplitter[]> item = queue.poll();

			SequentialFeatureSplitter best = fitNode(bagging(item.f1), queue.size());

			best.fillNode(item.f0);

			if (!best.canSplit()) {
				item.f0.makeLeaf();
				item.f0.makeLeafProb();
				continue;
			}

			split(item.f1, item.f0, best);
		}

		return root;
	}

	private SequentialFeatureSplitter fitNode(SequentialFeatureSplitter[] splitters, int leafNodeCount) {
		return fitNodeMultiThread(splitters, leafNodeCount);
	}

	private SequentialFeatureSplitter fitNodeSingleThread(SequentialFeatureSplitter[] splitters, int leafNodeCount) {
		double bestGain = Criteria.INVALID_GAIN;
		SequentialFeatureSplitter bestSplitter = null;
		for (SequentialFeatureSplitter splitter : splitters) {
			double gain = splitter.bestSplit(leafNodeCount);
			if (gain > bestGain || bestSplitter == null) {
				bestGain = gain;
				bestSplitter = splitter;
			}
		}

		return bestSplitter;
	}

	private SequentialFeatureSplitter fitNodeMultiThread(SequentialFeatureSplitter[] splitters, int leafNodeCount) {
		final int len = splitters.length;

		for (int i = 0; i < NUM_THREADS && i < len; ++i) {
			splitters[0].getPartition().resetThreadLocal(threadLocals[i]);
		}

		double bestGain = Criteria.INVALID_GAIN;
		SequentialFeatureSplitter bestSplitter = null;

		for (int i = 0; i < len; i += NUM_THREADS) {
			futures.clear();

			for (int j = 0; j < NUM_THREADS && i + j < len; ++j) {
				final SequentialFeatureSplitter splitter = splitters[i + j];
				splitter.setPartition(threadLocals[j]);

				futures.add(executorService
					.submit(() -> splitter.bestSplit(leafNodeCount)));
			}

			for (int j = 0; j < NUM_THREADS && i + j < len; ++j) {
				double gain;
				try {
					gain = futures.get(j).get();
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}

				if (gain > bestGain || bestSplitter == null) {
					bestGain = gain;
					bestSplitter = splitters[i + j];
				}
			}
		}

		return bestSplitter;
	}

	private void split(SequentialFeatureSplitter[] all, Node node, SequentialFeatureSplitter best) {
		SequentialFeatureSplitter[][] childSplitters = (SequentialFeatureSplitter[][]) best.split(all);
		Node[] nextNodes = new Node[childSplitters.length];

		for (int i = 0; i < childSplitters.length; ++i) {
			nextNodes[i] = new Node();
			queue.add(Tuple2.of(nextNodes[i], childSplitters[i]));
		}

		node.setNextNodes(nextNodes);
	}

	private SequentialFeatureSplitter[] bagging(SequentialFeatureSplitter[] splitters) {
		int size = Math.max(1,
			Math.min(
				(int) (params.get(RandomForestTrainParams.FEATURE_SUBSAMPLING_RATIO) * splitters.length),
				Math.min(
					splitters.length,
					params.get(RandomForestTrainParams.NUM_SUBSET_FEATURES)
				)
			)
		);

		if (size != splitters.length) {
			shuffle(splitters, random);
		}

		return Arrays.copyOf(splitters, size);
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
		ArrayList <Tuple2 <Integer, Double>> dataIndices = new ArrayList <>(data.m);
		for (int i = 0; i < data.m; ++i) {
			dataIndices.add(Tuple2.of(i, data.weights[i]));
		}

		return new SequentialPartition(dataIndices);
	}

	private SequentialFeatureSplitter[] initSplitters(SequentialPartition partition) {
		SequentialFeatureSplitter[] splitters = new SequentialFeatureSplitter[data.featureMetas.length];
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
