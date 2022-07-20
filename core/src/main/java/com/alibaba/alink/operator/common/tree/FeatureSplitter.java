package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinInfoGain;
import com.alibaba.alink.params.shared.tree.HasMinSampleRatioPerChild;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;

public abstract class FeatureSplitter implements Cloneable {
	// params and meta.
	protected final Params params;
	protected final FeatureMeta featureMeta;

	// pruning parameter
	protected final int minSamplesPerLeaf;
	protected final double minSampleRatioPerChild;
	protected final double minInfoGain;
	protected final int maxLeaves;

	// best split information.
	protected boolean canSplit = false;
	protected boolean counted = false;
	protected double bestGain;
	protected Criteria total;
	protected Criteria missing;
	protected int depth = 1;

	public FeatureSplitter(Params params, FeatureMeta featureMeta) {
		this.params = params;
		this.featureMeta = featureMeta;

		this.minSamplesPerLeaf = params.get(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF);
		this.minSampleRatioPerChild = params.get(HasMinSampleRatioPerChild.MIN_SAMPLE_RATIO_PERCHILD);
		this.minInfoGain = params.get(HasMinInfoGain.MIN_INFO_GAIN);
		this.maxLeaves = params.get(HasMaxLeaves.MAX_LEAVES);
	}

	public boolean canSplit() {
		return canSplit;
	}

	@Override
	public FeatureSplitter clone() {
		try {
			return (FeatureSplitter) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AkIllegalStateException("Can not clone FeatureSplitter.", e);
		}
	}

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

	public abstract double bestSplit(int curLeafCount);

	public abstract FeatureSplitter[][] split(FeatureSplitter[] splitters);

	protected abstract void count();

	protected abstract void fillNodeSplitPoint(Node node);
}
