package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.FeatureSplitter;
import com.alibaba.alink.operator.common.tree.LabelAccessor;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;

/**
 * FeatureSplitter.
 */
public abstract class SequentialFeatureSplitter extends FeatureSplitter {

	// data.
	protected DenseData data;
	protected SequentialPartition partition;
	protected final int maxDepth;

	public SequentialFeatureSplitter(
		Params params, DenseData data, FeatureMeta featureMeta, SequentialPartition partition) {
		super(params, featureMeta);
		this.data = data;
		this.partition = partition;

		this.maxDepth = params.get(HasMaxDepth.MAX_DEPTH);
	}

	// use for multi threads.
	public SequentialPartition getPartition() {
		return partition;
	}

	// use for multi threads.
	public void setPartition(SequentialPartition partition) {
		this.partition = partition;
	}

	SequentialFeatureSplitter[][] split(FeatureSplitter[] splitters, SequentialPartition[] childPartitions) {

		int length = childPartitions.length;
		int childSplittersLength = splitters.length;

		SequentialFeatureSplitter[][] childSplitters = new SequentialFeatureSplitter[length][];

		for (int i = 0; i < length; ++i) {
			SequentialFeatureSplitter[] childSplitter = new SequentialFeatureSplitter[splitters.length];

			for (int j = 0; j < childSplittersLength; ++j) {
				childSplitter[j] = (SequentialFeatureSplitter) splitters[j].clone();
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

	private class ClassificationLabelAccessor extends LabelAccessor {
		private Criteria.ClassificationCriteria criteria;
		private int[] labelValues = data.getLabelValues();

		ClassificationLabelAccessor(Criteria.ClassificationCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public int size() {
			return partition.dataIndices.size();
		}

		@Override
		public void add(int index) {
			criteria.add(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1,
				1
			);
		}

		@Override
		public void sub(int index) {
			int[] labelValues = data.getLabelValues();

			criteria.subtract(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1,
				1
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
		public int size() {
			return partition.dataIndices.size();
		}

		@Override
		public void add(int index) {
			criteria.add(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1,
				1
			);
		}

		@Override
		public void sub(int index) {
			criteria.subtract(
				labelValues[partition.dataIndices.get(index).f0],
				partition.dataIndices.get(index).f1,
				1
			);
		}
	}
}
