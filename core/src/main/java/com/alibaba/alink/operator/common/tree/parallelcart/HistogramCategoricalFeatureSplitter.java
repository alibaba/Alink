package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.LabelAccessor;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.GBMTreeSplitCriteria;

import java.util.Arrays;

public final class HistogramCategoricalFeatureSplitter extends HistogramFeatureSplitter {
	private int[] splitPoint;
	private Integer[] compareIndex4Categorical;

	public HistogramCategoricalFeatureSplitter(
		Params params, FeatureMeta featureMeta, Integer[] compareIndex4Categorical) {
		super(params, featureMeta);

		this.compareIndex4Categorical = compareIndex4Categorical;
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

		LabelAccessor accessor = labelAccessorOf(total);

		for (int i = 0; i < accessor.size(); ++i) {
			compareIndex4Categorical[i] = i;
		}

		((CategoricalLabelSortable) accessor).sort4Categorical(compareIndex4Categorical, 0, accessor.size());

		// iterate the feature and calculate the best split point.
		Criteria left = criteriaOf();
		Criteria right = total.clone();

		LabelAccessor totalLabelAccessor = labelAccessorOf(total);
		LabelAccessor leftLabelAccessor = labelAccessorOf(left);
		LabelAccessor rightLabelAccessor = labelAccessorOf(right);

		bestGain = Criteria.INVALID_GAIN;
		missingSplit = null;

		int localSplitPoint = 0;
		double localBestGain = Criteria.INVALID_GAIN;
		int localMissingSplit = 1;
		for (int i = 0; i < totalLabelAccessor.size(); ++i) {
			if (useMissing && compareIndex4Categorical[i] == featureMeta.getMissingIndex()) {
				continue;
			}

			if (i == 0) {
				leftLabelAccessor.add(compareIndex4Categorical[i]);
				rightLabelAccessor.sub(compareIndex4Categorical[i]);
				continue;
			}

			double gain = gain(left, right);

			if (useMissing) {
				localMissingSplit = 1;
				double leftGain = gain(left.clone().add(missing), right.clone().subtract(missing));

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
				} else {
					bestLeft = left.clone();
					bestRight = right.clone();
				}

				localSplitPoint = i - 1;
				bestGain = ((GBMTreeSplitCriteria) total).actualGain(bestLeft, bestRight);
				canSplit = true;
			}

			leftLabelAccessor.add(compareIndex4Categorical[i]);
			rightLabelAccessor.sub(compareIndex4Categorical[i]);
		}

		if (canSplit) {
			splitPoint = new int[featureMeta.getNumCategorical()];
			Arrays.fill(splitPoint, -1);

			if (useMissing) {
				for (int i = 0; i <= localSplitPoint; ++i) {
					if (compareIndex4Categorical[i] < featureMeta.getMissingIndex()) {
						splitPoint[compareIndex4Categorical[i]] = 0;
					} else if (compareIndex4Categorical[i] > featureMeta.getMissingIndex()) {
						splitPoint[compareIndex4Categorical[i] - 1] = 0;
					}
				}

				for (int i = localSplitPoint + 1; i < totalLabelAccessor.size(); ++i) {
					if (compareIndex4Categorical[i] < featureMeta.getMissingIndex()) {
						splitPoint[compareIndex4Categorical[i]] = 1;
					} else if (compareIndex4Categorical[i] > featureMeta.getMissingIndex()) {
						splitPoint[compareIndex4Categorical[i] - 1] = 1;
					}
				}
			} else {
				for (int i = 0; i <= localSplitPoint; ++i) {
					splitPoint[compareIndex4Categorical[i]] = 0;
				}

				for (int i = localSplitPoint + 1; i < featureMeta.getNumCategorical(); ++i) {
					splitPoint[compareIndex4Categorical[i]] = 1;
				}
			}
		}

		return bestGain;
	}

	@Override
	protected void fillNodeSplitPoint(Node node) {
		node.setCategoricalSplit(splitPoint);
		node.setMissingSplit(missingSplit);
	}
}
