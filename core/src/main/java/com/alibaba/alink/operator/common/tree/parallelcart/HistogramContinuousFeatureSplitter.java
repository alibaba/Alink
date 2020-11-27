package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.LabelAccessor;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.GBMTreeSplitCriteria;

public final class HistogramContinuousFeatureSplitter extends HistogramFeatureSplitter {
	private int splitPoint;

	public HistogramContinuousFeatureSplitter(
		Params params, FeatureMeta featureMeta) {
		super(params, featureMeta);
	}

	private double gain(Criteria left, Criteria right) {
		if ((!useInstanceCnt &&
			(minSamplesPerLeaf > left.getWeightSum()
				|| minSamplesPerLeaf > right.getWeightSum()))
			|| (useInstanceCnt &&
			(minSamplesPerLeaf > left.getNumInstances()
				|| minSamplesPerLeaf > right.getNumInstances()))
			|| minSampleRatioPerChild >
			(
				((double) left.getNumInstances()) / ((double) total.getNumInstances())
			)
			|| minSampleRatioPerChild >
			(
				((double) right.getNumInstances()) / ((double) total.getNumInstances())
			)) {
			return Criteria.INVALID_GAIN;
		}

		return total.gain(left, right);
	}

	@Override
	public double bestSplit(int curLeafCount) {
		// prune.
		if (depth >= maxDepth
			|| curLeafCount + 2 > maxLeaves) {
			return Criteria.INVALID_GAIN;
		}

		count();

		if (total.getNumInstances() <= minSamplesPerLeaf) {
			return Criteria.INVALID_GAIN;
		}

		// iterate the feature and calculate the best split point.
		GBMTreeSplitCriteria left = criteriaOf();
		GBMTreeSplitCriteria right = (GBMTreeSplitCriteria) total.clone();

		LabelAccessor totalLabelAccessor = labelAccessorOf(total);

		LabelAccessor leftLabelAccessor = labelAccessorOf(left);
		LabelAccessor rightLabelAccessor = labelAccessorOf(right);

		bestGain = Criteria.INVALID_GAIN;
		missingSplit = null;

		double localBestGain = Criteria.INVALID_GAIN;
		boolean first = true;
		int localMissingSplit = 1;
		for (int i = 0; i < totalLabelAccessor.size(); ++i) {
			if (useMissing && i == featureMeta.getMissingIndex()) {
				continue;
			}

			if (first) {
				leftLabelAccessor.add(i);
				rightLabelAccessor.sub(i);
				first = false;
				continue;
			}

			double gain = gain(left, right);

			if (useMissing) {
				localMissingSplit = 1;
				double leftGain = gain(
					left.clone().add(missing),
					right.clone().subtract(missing)
				);

				if (gain < leftGain) {
					gain = leftGain;
					localMissingSplit = 0;
				}
			}

			if (gain > localBestGain && gain >= minInfoGain) {

				localBestGain = gain;
				if (useMissing) {
					if (localMissingSplit == 0) {
						bestLeft = left.clone().add(missing);
						bestRight = right.clone().subtract(missing);
					} else {
						bestLeft = left.clone();
						bestRight = right.clone();
					}

					missingSplit = new int[] {localMissingSplit};
					splitPoint = i - 1 > featureMeta.getMissingIndex() ? i - 2 : i - 1;
				} else {
					bestLeft = left.clone();
					bestRight = right.clone();

					splitPoint = i - 1;
				}

				bestGain = ((GBMTreeSplitCriteria) total).actualGain(bestLeft, bestRight);
				canSplit = true;
			}

			leftLabelAccessor.add(i);
			rightLabelAccessor.sub(i);
		}

		return bestGain;
	}

	@Override
	protected void fillNodeSplitPoint(Node node) {
		node.setContinuousSplit(splitPoint);
		node.setMissingSplit(missingSplit);
	}
}
