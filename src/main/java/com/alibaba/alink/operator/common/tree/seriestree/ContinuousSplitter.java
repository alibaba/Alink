package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;

/**
 * ContinuousSplitter.
 */
public class ContinuousSplitter extends FeatureSplitter {
	private double splitPoint;
	private Criteria bestLeft;
	private Criteria bestRight;

	public ContinuousSplitter(
		Params params,
		DenseData data,
		FeatureMeta featureMeta,
		SequentialPartition partition) {
		super(params, data, featureMeta, partition);
	}

	@Override
	public double bestSplit(int curLeafCount) {
		// prune.
		if (depth >= maxDepth
			|| curLeafCount + 2 >= maxLeaves
			|| partition.dataIndices.size() <= minSamplesPerLeaf) {
			return Criteria.INVALID_GAIN;
		}

		count();

		int size = partition.dataIndices.size();

		// if all instances are missing, it prune the tree.
		if (missing.getNumInstances() == size) {
			return Criteria.INVALID_GAIN;
		}

		// sort the continuous feature.
		partition.sort(
			data.getFeatureValues(featureMeta.getIndex())
		);

		// iterate the feature and calculate the best split point.
		Criteria left = criteriaOf();
		LabelAccessor leftLabelAccessor = labelAccessorOf(left);
		Criteria right = total.clone();
		LabelAccessor rightLabelAccessor = labelAccessorOf(right);

		double[] featureValues = data.getFeatureValues(featureMeta.getIndex());

		double before = Double.NaN;
		bestGain = Criteria.INVALID_GAIN;
		for (int i = 0; i < total.getNumInstances(); ++i) {
			double featureValue = featureValues[partition.dataIndices.get(i).f0];

			if (featureValue - before < 1e-15
				|| i == 0
				|| minSamplesPerLeaf > (i + missing.getNumInstances())
				|| minSamplesPerLeaf > (total.getNumInstances() - i + missing.getNumInstances())
				|| minSampleRatioPerChild >
				(
					((double) left.getNumInstances() + missing.getNumInstances())
						/ ((double) total.getNumInstances() + missing.getNumInstances())
				)
				|| minSampleRatioPerChild >
				(
					((double) right.getNumInstances() + (double) missing.getNumInstances())
						/ ((double) total.getNumInstances() + (double) missing.getNumInstances())
				)) {
				leftLabelAccessor.add(i);
				rightLabelAccessor.sub(i);
				before = featureValue;
				continue;
			}

			double gain = total.gain(left, right);

			if (gain > bestGain
				&& gain >= minInfoGain) {
				bestGain = gain;
				canSplit = true;
				splitPoint = (before + featureValue) / 2.0;
				bestLeft = left.clone();
				bestRight = right.clone();
			}

			leftLabelAccessor.add(i);
			rightLabelAccessor.sub(i);
			before = featureValue;
		}

		return bestGain;
	}

	@Override
	public FeatureSplitter[][] split(FeatureSplitter[] splitters) {
		if (!canSplit) {
			throw new IllegalStateException("The feature splitter should be calculated by `bestSplit`");
		}

		return split(
			splitters,
			partition.splitContinuous(
				data.getFeatureValues(featureMeta.getIndex()),
				splitPoint,
				bestLeft.getWeightSum() / total.getWeightSum(),
				bestRight.getWeightSum() / total.getWeightSum()
			)
		);
	}

	@Override
	protected void count() {
		if (counted) {
			return;
		}

		// stat the label at this feature.
		int size = partition.dataIndices.size();
		double[] featureValues = data.getFeatureValues(featureMeta.getIndex());

		total = criteriaOf();
		missing = criteriaOf();
		LabelAccessor validTotalLabelAccessor = labelAccessorOf(total);
		LabelAccessor missingTotalLabelAccessor = labelAccessorOf(missing);

		for (int i = 0; i < size; ++i) {
			double featureValue = featureValues[partition.dataIndices.get(i).f0];
			if (DenseData.isContinuousMissValue(featureValue)) {
				missingTotalLabelAccessor.add(i);
			} else {
				validTotalLabelAccessor.add(i);
			}
		}

		counted = true;
	}

	@Override
	protected void fillNodeSplitPoint(Node node) {
		node.setContinuousSplit(splitPoint);
	}
}
