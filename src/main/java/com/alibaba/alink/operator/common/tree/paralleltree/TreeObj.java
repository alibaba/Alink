package com.alibaba.alink.operator.common.tree.paralleltree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxMemoryInMB;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;
import com.alibaba.alink.params.shared.tree.HasSeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * use group wise
 */
public abstract class TreeObj<LABELARRAY> implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(TreeObj.class);

	public final static ParamInfo<Integer> N_LOCAL_ROW = ParamInfoFactory
		.createParamInfo("nLocalRow", Integer.class)
		.setDescription("n local row")
		.setRequired()
		.build();

	public final static ParamInfo<Integer> TASK_ID = ParamInfoFactory
		.createParamInfo("taskId", Integer.class)
		.setDescription("task id")
		.setRequired()
		.build();

	public final static ParamInfo<Integer> NUM_OF_SUBTASKS = ParamInfoFactory
		.createParamInfo("numOfSubTasks", Integer.class)
		.setDescription("numOfSubTasks")
		.setRequired()
		.build();

	public static final int TRANSFER_BUF_SIZE = 1024 * 4;

	protected int maxHistBufferSize;
	protected int maxLoopBufferSize;

	protected int taskId;
	protected int numOfSubTasks;

	/*
	 * col major
	 */
	protected int[] features;
	protected int[] partitions;

	protected double[] hist;
	protected double[] minusHist;
	protected int[] randomShuffleBuf;

	protected BufferPool parentHistPool;

	protected List<Node> roots = new LinkedList<>();

	protected Deque<NodeInfoPair> queue;

	protected NodeInfoPair[] loopBuffer;
	protected int loopBufferSize;

	protected Params params;

	protected int nFeatureCol;
	protected int numLocalRow;
	protected int numLocalBaggingRow;
	protected int nBin;
	protected double subSamplingRatio;
	protected int numTrees;
	protected boolean rootBagging = false;
	protected FeatureMeta[] featureMetas;
	protected FeatureMeta labelMeta;

	protected QuantileDiscretizerModelDataConverter quantileDiscretizerModel;
	protected Random randomSample;
	protected Random randomFeature;

	protected final int maxDepth;
	protected final int minSamplesPerLeaf;

	public TreeObj(
		Params params,
		QuantileDiscretizerModelDataConverter qModel,
		FeatureMeta[] featureMetas,
		FeatureMeta labelMeta) {
		this.params = params;
		this.featureMetas = featureMetas;
		this.labelMeta = labelMeta;
		this.quantileDiscretizerModel = qModel;

		numTrees = params.get(RandomForestTrainParams.NUM_TREES);
		nFeatureCol = params.get(RandomForestTrainParams.FEATURE_COLS).length;
		numLocalRow = params.get(N_LOCAL_ROW);
		nBin = params.get(HasMaxBins.MAX_BINS);
		taskId = params.get(TASK_ID);
		subSamplingRatio = params.get(RandomForestTrainParams.SUBSAMPLING_RATIO);
		numOfSubTasks = params.get(NUM_OF_SUBTASKS);
		maxHistBufferSize = params.get(HasMaxMemoryInMB.MAX_MEMORY_IN_MB) * 1024 * 1024 / 8;
		numLocalBaggingRow = (int) Math.min(numLocalRow, Math.ceil(numLocalRow * subSamplingRatio));

		randomSample = new Random(params.get(HasSeed.SEED));
		randomFeature = new Random(params.get(HasSeed.SEED));
		maxDepth = params.get(HasMaxDepth.MAX_DEPTH);
		minSamplesPerLeaf = params.get(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF);
	}

	public final static Node ofNode() {
		return new Node();
	}

	public final static Node ofNode(NodeInfoPair pair, boolean left) {
		Node ret;
		if (pair.root()) {
			ret = pair.parentNode;
		} else {

			ret = ofNode();

			if (pair.parentNode.getNextNodes() == null) {
				pair.parentNode.setNextNodes(new Node[2]);
			}

			if (left) {
				pair.parentNode.getNextNodes()[0] = ret;
			} else {
				pair.parentNode.getNextNodes()[1] = ret;
			}
		}

		return ret;
	}

	public void setFeatures(int[] features) {
		this.features = features;
	}

	void initBuffer() {
		maxLoopBufferSize = maxHistBufferSize / lenStatUnit();

		queue = new ArrayDeque<>(maxLoopBufferSize * 2);
		loopBuffer = new NodeInfoPair[maxLoopBufferSize];
		loopBufferSize = 0;

		minusHist = new double[maxHistBufferSize * 2];
		if (!useStatPair()) {
			parentHistPool = new BufferPool(lenPerStat());
		}

		initPartitions();

		randomShuffleBuf = new int[nFeatureCol];
		for (int i = 0; i < nFeatureCol; ++i) {
			randomShuffleBuf[i] = i;
		}
	}

	public void setHist(double[] hist) {
		this.hist = hist;
	}

	public int getMaxHistBufferSize() {
		return maxHistBufferSize;
	}

	protected void initPartitions() {
		int[] partition4Shuffle = new int[numLocalRow];

		for (int i = 0; i < numLocalRow; ++i) {
			partition4Shuffle[i] = i;
		}

		partitions = new int[numLocalBaggingRow * numTrees];

		for (int i = 0; i < numTrees; ++i) {
			shuffle(partition4Shuffle, randomSample);

			int start = i * numLocalBaggingRow;

			System.arraycopy(partition4Shuffle, 0, partitions, start, numLocalBaggingRow);
		}
	}

	public final NodeInfoPair ofNodeInfoPair(Node node, int minusId, NodeInfoPair p) {
		NodeInfoPair children = new NodeInfoPair();

		if (minusId >= 0) {
			if (!useStatPair()) {
				int parentId = parentHistPool.nextValidId();
				double[] parent = parentHistPool.get(parentId);
				System.arraycopy(minusHist, minusId * lenStatUnit(), parent, 0, lenStatUnit());
				children.parentQueueId = parentId;
			}
		} else {
			children.parentQueueId = minusId;
		}

		if (p == null) {
			children.depth = 1;
		} else {
			children.depth = p.depth + 1;
			children.baggingFeatures = p.baggingFeatures;
		}

		children.parentNode = node;

		return children;
	}

	public abstract int lenPerStat();

	public int lenStatUnit() {
		if (useStatPair()) {
			return lenPerStat() * 2;
		}

		return lenPerStat();
	}

	public final double[] hist() {
		return hist;
	}

	public final int histLen() {
		return loopBufferSize * lenStatUnit();
	}

	abstract public void setLabels(LABELARRAY values);

	abstract public void stat(int loop, int start, int end, int bufStart, int bufEnd);

	public final Deque<NodeInfoPair> getQueue() {
		return queue;
	}

	public final void initialRoot() {
		for (int i = 0; i < numTrees; ++i) {
			Node root = TreeObj.ofNode();
			addNodeInfoPair(ofNodeInfoPair(root, -1, null)
				.initialRoot(i * numLocalBaggingRow, (i + 1) * numLocalBaggingRow));
			roots.add(root);
		}
	}

	public final void addNodeInfoPair(NodeInfoPair info) {
		queue.addLast(info);
	}

	public final void determineLoopNode() {
		int memUsage = 0;
		loopBufferSize = 0;
		while (true) {
			if (loopBufferSize + 1 > maxLoopBufferSize) {
				return;
			}

			NodeInfoPair pair = queue.peekFirst();

			if (pair == null) {
				return;
			}

			memUsage += lenStatUnit();

			if (memUsage > maxHistBufferSize) {
				return;
			}

			loopBuffer[loopBufferSize] = queue.pollFirst();
			loopBufferSize++;
		}
	}

	public final void initialLoop() {
		int baggingFeatureCount = baggingFeatureCount();
		if (baggingFeatureCount != nFeatureCol) {
			for (int i = 0; i < loopBufferSize; ++i) {
				if (loopBuffer[i].baggingFeatures != null && rootBagging) {
					continue;
				}

				loopBuffer[i].baggingFeatures = new int[baggingFeatureCount];

				shuffle(randomShuffleBuf, randomFeature);

				System.arraycopy(randomShuffleBuf, 0, loopBuffer[i].baggingFeatures, 0, baggingFeatureCount);

				LOG.info("taskId: {}, loopBuffer: {}, randomFeatureEnd", taskId, JsonConverter.gson.toJson(loopBuffer[i]));
			}
		}
	}

	public final void shuffle(int[] array, Random random) {
		for (int i = array.length - 1; i > 0; --i) {
			int idx = random.nextInt(i + 1);

			if (idx == i) {
				continue;
			}

			int tmp = array[idx];
			array[idx] = array[i];
			array[i] = tmp;
		}
	}

	public final int stat() {
		for (int i = 0; i < loopBufferSize; ++i) {
			int bufStart = i * lenStatUnit();
			int bufEnd = bufStart + lenPerStat();
			stat(
				i, loopBuffer[i].small.start,
				loopBuffer[i].small.end,
				bufStart,
				bufEnd
			);

			if (useStatPair() && loopBuffer[i].big != null) {
				stat(
					i, loopBuffer[i].big.start,
					loopBuffer[i].big.end,
					bufStart + lenPerStat(),
					bufEnd + lenPerStat()
				);
			}
		}

		return loopBufferSize;
	}

	abstract public void bestSplit(Node node, int minusId, NodeInfoPair pair) throws Exception;

	public final void bestSplit() throws Exception {
		double[] stat = hist;

		if (useStatPair()) {
			System.arraycopy(stat, 0, minusHist, 0, histLen());
		}

		int lenPerNode = lenStatUnit();
		int minusId = 0;
		for (int i = 0; i < loopBufferSize; ++i) {
			int statStart = i * lenPerNode;
			if (!useStatPair()) {
				System.arraycopy(stat, statStart, minusHist, minusId * lenPerNode, lenPerNode);
			}

			Node left = ofNode(loopBuffer[i], true);
			bestSplit(left, minusId, loopBuffer[i]);
			split(left, loopBuffer[i], true, minusId);
			replaceWithActual(left);

			if (loopBuffer[i].big != null) {
				if (!useStatPair()) {
					double[] parent = parentHistPool.get(loopBuffer[i].parentQueueId);
					int bigStart = (minusId + 1) * lenPerNode;
					for (int j = 0; j < lenPerNode; ++j) {
						minusHist[bigStart + j] = parent[j] - stat[statStart + j];
					}

					parentHistPool.release(loopBuffer[i].parentQueueId);
				}

				minusId += 1;

				Node right = ofNode(loopBuffer[i], false);
				bestSplit(right, minusId, loopBuffer[i]);
				split(right, loopBuffer[i], false, minusId);
				replaceWithActual(right);
			} else if (useStatPair()) {
				minusId += 1;
			}

			minusId += 1;
		}
	}

	public final void split(Node node, NodeInfoPair infoPair, boolean left, int minusId) {
		if (node.isLeaf()) {
			node.makeLeafProb();
			return;
		}

		NodeInfoPair.Partition partition;
		if (left) {
			partition = infoPair.small;
		} else {
			partition = infoPair.big;
		}

		NodeInfoPair children = ofNodeInfoPair(node, minusId, infoPair);
		split(node, partition, children);
		addNodeInfoPair(children);
	}

	public final void replaceWithActual(Node node) {
		if (node.isLeaf() || node.getCategoricalSplit() != null) {
			return;
		}

		node.setContinuousSplit(quantileDiscretizerModel
			.getData()
			.get(featureMetas[node.getFeatureIndex()]
				.getName()
			)[(int) node.getContinuousSplit()]
		);
	}

	public final boolean left(int featureStart, int partitionIdx, Node node) {
		int val = features[featureStart + partitions[partitionIdx]];

		if (node.getCategoricalSplit() == null) {
			return val <= node.getContinuousSplit();
		} else {
			return node.getCategoricalSplit()[val] == 0;
		}
	}

	public final void split(Node node, NodeInfoPair.Partition partition, NodeInfoPair children) {
		if (node.isLeaf()) {
			return;
		}

		int splitStart = node.getFeatureIndex() * numLocalRow;

		int lstart = partition.start;
		int lend = partition.end;
		lend--;
		while (lstart <= lend) {
			while (lstart <= lend &&
				left(splitStart, lstart, node)) {
				lstart++;
			}

			while (lstart <= lend &&
				!left(splitStart, lend, node)) {
				lend--;
			}

			if (lstart < lend) {
				int tmp = partitions[lstart];
				partitions[lstart] = partitions[lend];
				partitions[lend] = tmp;
			}
		}

		children.small = new NodeInfoPair.Partition();
		children.big = new NodeInfoPair.Partition();

		children.small.start = partition.start;
		children.small.end = lstart;
		children.big.start = lstart;
		children.big.end = partition.end;
	}

	public final boolean terminationCriterion() {
		return queue.isEmpty();
	}

	public final List<Node> getRoots() {
		return roots;
	}

	private boolean useStatPair() {
		return baggingFeatureCount() != nFeatureCol && !rootBagging;
	}


	protected int baggingFeatureCount() {
		return Math.max(1,
			Math.min(
				(int) (params.get(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO) * nFeatureCol),
				nFeatureCol
			)
		);
	}

}
