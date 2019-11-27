package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinInfoGain;
import com.alibaba.alink.params.shared.tree.HasMinSampleRatioPerChild;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;

/**
 * FeatureSplitter.
 */
public abstract class FeatureSplitter implements Cloneable {

	// params and data.
	protected Params params;
	protected DenseData data;
	protected FeatureMeta featureMeta;
	protected SequentialPartition partition;

	// pruning parameter
	protected final int minSamplesPerLeaf;
	protected final double minSampleRatioPerChild;
	protected final double minInfoGain;
	protected final int maxDepth;
	protected final int maxLeaves;

	// best split information.
	protected boolean canSplit = false;
	protected boolean counted = false;
	protected double bestGain;
	protected Criteria total;
	protected Criteria missing;
	protected int depth = 1;

	public FeatureSplitter(
		Params params, DenseData data, FeatureMeta featureMeta, SequentialPartition partition) {
		this.params = params;
		this.data = data;
		this.featureMeta = featureMeta;
		this.partition = partition;

		this.minSamplesPerLeaf = params.get(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF);
		this.minSampleRatioPerChild = params.get(HasMinSampleRatioPerChild.MIN_SAMPLE_RATIO_PERCHILD);
		this.minInfoGain = params.get(HasMinInfoGain.MIN_INFO_GAIN);
		this.maxDepth = params.get(HasMaxDepth.MAX_DEPTH);
		this.maxLeaves = params.get(HasMaxLeaves.MAX_LEAVES);
	}

	public abstract double bestSplit(int curLeafCount);

	public abstract FeatureSplitter[][] split(FeatureSplitter[] splitters);

	public void fillNode(Node node) {
		count();
		Preconditions.checkNotNull(total);
		Preconditions.checkNotNull(missing);
		node.setCounter(total.clone().add(missing).toLabelCounter());
		if (canSplit) {
			node.setFeatureIndex(featureMeta.getIndex())
				.setGain(bestGain);
			fillNodeSplitPoint(node);
		}
	}

	@Override
	public FeatureSplitter clone() {
		try {
			return (FeatureSplitter) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException("Can not clone FeatureSplitter.", e);
		}
	}

	protected abstract void count();
	protected abstract void fillNodeSplitPoint(Node node);

	FeatureSplitter[][] split(FeatureSplitter[] splitters, SequentialPartition[] childPartitions) {

		int length = childPartitions.length;
		int childSplittersLength = splitters.length;

		FeatureSplitter[][] childSplitters = new FeatureSplitter[length][];

		for (int i = 0; i < length; ++i) {
			FeatureSplitter[] childSplitter = new FeatureSplitter[splitters.length];

			for (int j = 0; j < childSplittersLength; ++j) {
				childSplitter[j] = splitters[j].clone();
				childSplitter[j].canSplit = false;
				childSplitter[j].counted = false;
				childSplitter[j].partition = childPartitions[i];
				childSplitter[j].depth = depth + 1;
			}

			childSplitters[i] = childSplitter;
		}

		return childSplitters;
	}

	Criteria criteriaOf() {
		switch (params.get(Criteria.Gain.GAIN)) {
			case GINI:
				return new Criteria.Gini(
					0, 0, new double[data.labelMeta.getNumCategorical()]
				);
			case INFOGAIN:
				return new Criteria.InfoGain(
					0, 0, new double[data.labelMeta.getNumCategorical()]
				);
			case INFOGAINRATIO:
				return new Criteria.InfoGainRatio(
					0, 0, new double[data.labelMeta.getNumCategorical()]
				);
			case MSE:
				return new Criteria.MSE(0, 0, 0, 0);
			default:
				throw new IllegalStateException("There should be set the gain type");
		}
	}

	LabelAccessor labelAccessorOf(Criteria criteria) {
		if (criteria instanceof Criteria.ClassificationCriteria) {
			return new ClassificationLabelAccessor((Criteria.ClassificationCriteria) criteria);
		} else {
			return new RegressionLabelAccessor((Criteria.RegressionCriteria) criteria);
		}
	}

	abstract static class LabelAccessor {
		public abstract void add(int index);
		public abstract void sub(int index);
	}

	private class ClassificationLabelAccessor extends LabelAccessor {
		private Criteria.ClassificationCriteria criteria;
		private int[] labelValues = data.getLabelValues();

		ClassificationLabelAccessor(Criteria.ClassificationCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public void add(int index) {
			criteria.add(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1
			);
		}

		@Override
		public void sub(int index) {
			int[] labelValues = data.getLabelValues();

			criteria.subtract(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1
			);
		}
	}

	private class RegressionLabelAccessor extends LabelAccessor {
		private Criteria.RegressionCriteria criteria;
		private double[] labelValues = data.getLabelValues();

		RegressionLabelAccessor(Criteria.RegressionCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public void add(int index) {
			criteria.add(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1
			);
		}

		@Override
		public void sub(int index) {
			criteria.subtract(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1
			);
		}
	}
}
