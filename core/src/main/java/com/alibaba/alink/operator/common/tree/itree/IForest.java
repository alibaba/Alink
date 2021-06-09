package com.alibaba.alink.operator.common.tree.itree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinSampleRatioPerChild;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;

public class IForest {

	public final static ParamInfo <Double> ISOLATION_GROUP_MAXCOUNT = ParamInfoFactory
		.createParamInfo("isolationGroupMaxCount", Double.class)
		.setDescription("isolationGroupMaxCount")
		.setRequired()
		.build();

	private final double[][] data;
	private final Params params;
	private final int[] sampleIndices;
	private final int nSamples;
	private final ArrayList <Integer> shuffleBuffer;
	Deque <QueueItem> nodeQueue = new ArrayDeque <>();

	private final int minSamplesPerLeaf;
	private final double minSampleRatioPerChild;
	private final int maxDepth;
	private final int maxLeaves;

	static class SlicedPartition {
		int start;
		int end;

		public SlicedPartition(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}

	public IForest(double[][] data, Params params) {
		this.data = data;
		this.params = params;

		nSamples = data[0].length;

		sampleIndices = new int[nSamples];

		for (int i = 0; i < nSamples; ++i) {
			sampleIndices[i] = i;
		}

		shuffleBuffer = new ArrayList <>(data.length);
		for (int i = 0; i < data.length; ++i) {
			shuffleBuffer.add(i);
		}

		this.minSamplesPerLeaf = params.get(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF);
		this.minSampleRatioPerChild = params.get(HasMinSampleRatioPerChild.MIN_SAMPLE_RATIO_PERCHILD);
		this.maxDepth = params.get(HasMaxDepth.MAX_DEPTH);
		this.maxLeaves = params.get(HasMaxLeaves.MAX_LEAVES);
	}

	static class QueueItem {
		Node node;
		int depth;
		SlicedPartition partition;

		public QueueItem(Node node, int depth, SlicedPartition partition) {
			this.node = node;
			this.depth = depth;
			this.partition = partition;
		}
	}

	public Node fit() {
		Node root = new Node();
		nodeQueue.push(new QueueItem(root, 1, new SlicedPartition(0, nSamples)));

		int leafNodeCount = 0;
		while (leafNodeCount <= params.get(HasMaxLeaves.MAX_LEAVES)) {
			if (nodeQueue.isEmpty()) {
				break;
			}

			QueueItem item = nodeQueue.poll();

			fitNode(item, leafNodeCount + nodeQueue.size());

			if (item.node.isLeaf()) {
				leafNodeCount++;
				continue;
			}

			split(item);
		}

		return root;
	}

	public void fitNode(QueueItem item, int leafNodeCount) {
		int totalSamples = item.partition.end - item.partition.start;

		item.node.setCounter(new LabelCounter(totalSamples, totalSamples, null));

		if (leafNodeCount + 2 >= maxLeaves) {
			item.node.makeLeaf();
			return;
		}

		if (item.depth >= maxDepth) {
			item.node.makeLeaf();
			return;
		}

		if (totalSamples <= minSamplesPerLeaf) {
			item.node.makeLeaf();
			return;
		}

		Collections.shuffle(shuffleBuffer);

		for (Integer featureIndex : shuffleBuffer) {
			double[] featureData = data[featureIndex];

			double min = Double.MAX_VALUE;
			double max = -Double.MAX_VALUE;

			for (int i = item.partition.start; i < item.partition.end; i++) {
				double value = featureData[sampleIndices[i]];

				if (min > value) {
					min = value;
				}

				if (max < value) {
					max = value;
				}
			}

			if (min == max) {
				continue;
			}

			double random;

			do {
				random = Math.random();
			} while (0.0 == random);

			double splitValue = min
				+ (max - min) * random;

			if (splitValue == min || splitValue == max) {
				continue;
			}

			int left = 0;
			for (int i = item.partition.start; i < item.partition.end; i++) {
				double value = featureData[sampleIndices[i]];

				if (value <= splitValue) {
					left++;
				}
			}

			int right = totalSamples - left;

			if (left < minSamplesPerLeaf
				|| right < minSamplesPerLeaf
				|| ((double) left / (double) totalSamples) < minSampleRatioPerChild
				|| ((double) right / (double) totalSamples) < minSampleRatioPerChild) {
				continue;
			}

			item.node.setFeatureIndex(featureIndex);
			item.node.setContinuousSplit(splitValue);
			return;
		}
	}

	public void split(QueueItem item) {
		int partitionMid = splitAndSwapData(
			item.partition,
			item.node.getFeatureIndex(),
			item.node.getContinuousSplit()
		);

		SlicedPartition leftPartition = new SlicedPartition(item.partition.start, partitionMid);
		SlicedPartition rightPartition = new SlicedPartition(partitionMid, item.partition.end);

		Node left = new Node();
		Node right = new Node();
		item.node.setNextNodes(new Node[] {left, right});

		nodeQueue.add(new QueueItem(left, item.depth + 1, leftPartition));
		nodeQueue.add(new QueueItem(right, item.depth + 1, rightPartition));
	}

	private int splitAndSwapData(
		SlicedPartition partition,
		int featureIndex,
		double splitPoint) {
		int left = partition.start;
		int right = partition.end - 1;

		while (left < right) {
			if (data[featureIndex][sampleIndices[left]] > splitPoint) {
				int tmp = sampleIndices[left];
				sampleIndices[left] = sampleIndices[right];
				sampleIndices[right] = tmp;
				right--;
			} else {
				left++;
			}
		}

		if (data[featureIndex][sampleIndices[left]] <= splitPoint) {
			right++;
		}

		return right;
	}
}
