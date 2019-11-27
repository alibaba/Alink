package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * CategoricalSplitter.
 */
public class CategoricalSplitter extends FeatureSplitter {
	private int[] splitPoint;
	private Criteria[] best;
	private Criteria[] categoricalCriteria;

	public CategoricalSplitter(
		Params params,
		DenseData data,
		FeatureMeta featureMeta,
		SequentialPartition partition) {
		super(params, data, featureMeta, partition);
	}

	@Override
	public double bestSplit(int curLeafCount) {
		if (depth >= params.get(HasMaxDepth.MAX_DEPTH)
			|| partition.dataIndices.size() <= params.get(HasMinSamplesPerLeaf.MIN_SAMPLES_PER_LEAF)) {
			return Criteria.INVALID_GAIN;
		}

		if (params.get(Criteria.Gain.GAIN) == Criteria.Gain.GINI
			|| params.get(Criteria.Gain.GAIN) == Criteria.Gain.MSE) {
			return bestSplitCart(curLeafCount);
		} else {
			return bestSplitInfo(curLeafCount);
		}
	}

	@Override
	public FeatureSplitter[][] split(FeatureSplitter[] splitters) {
		if (!canSplit) {
			throw new IllegalStateException("The feature splitter should be calculated by `bestSplit`");
		}

		double[] ratioOfChild = new double[best.length];

		for (int i = 0; i < best.length; ++i) {
			ratioOfChild[i] = best[i].getWeightSum() / total.getWeightSum();
		}

		return split(
			splitters,
			partition.splitCategorical(
				data.getFeatureValues(featureMeta.getIndex()),
				best.length,
				splitPoint,
				ratioOfChild
			)
		);
	}

	@Override
	protected void count() {
		if (counted) {
			return;
		}

		// stat the label at this feature.
		int categoricalSize = featureMeta.getNumCategorical();

		categoricalCriteria = new Criteria[categoricalSize];
		LabelAccessor[] categoricalLabelAccessor = new LabelAccessor[categoricalSize];

		for (int i = 0; i < categoricalSize; ++i) {
			categoricalCriteria[i] = criteriaOf();
			categoricalLabelAccessor[i] = labelAccessorOf(categoricalCriteria[i]);
		}

		missing = criteriaOf();
		LabelAccessor missingTotalLabelAccessor = labelAccessorOf(missing);

		int size = partition.dataIndices.size();
		int[] featureValues = data.getFeatureValues(featureMeta.getIndex());

		for (int i = 0; i < size; ++i) {
			int featureValue = featureValues[partition.dataIndices.get(i).f0];
			if (DenseData.isCategoricalMissValue(featureValue)) {
				missingTotalLabelAccessor.add(i);
			} else {
				categoricalLabelAccessor[featureValue].add(i);
			}
		}

		total = criteriaOf();
		for (int i = 0; i < categoricalSize; ++i) {
			total.add(categoricalCriteria[i]);
		}

		counted = true;
	}

	@Override
	protected void fillNodeSplitPoint(Node node) {
		node.setCategoricalSplit(splitPoint);
	}

	private double bestSplitCartIncrement() {
		int categoricalSize = featureMeta.getNumCategorical();

		List<Tuple2<Integer, Double>> probability0 = new ArrayList<>(categoricalSize);
		int[] categoricalSplitPoint = new int[categoricalSize];
		for (int i = 0; i < categoricalSize; ++i) {
			probability0.add(
				Tuple2.of(
					i,
					categoricalCriteria[i]
						.clone()
						.toLabelCounter()
						.normWithWeight()
						.getDistributions()[0]
				)
			);
			categoricalSplitPoint[i] = 1;
		}

		probability0.sort(Comparator.comparing(o -> o.f1));

		Criteria.Gini left = (Criteria.Gini) criteriaOf();
		Criteria.Gini right = (Criteria.Gini) total.clone();

		bestGain = Criteria.INVALID_GAIN;
		splitPoint = new int[categoricalSize];
		best = new Criteria[2];

		for (int i = 0; i < categoricalSize; ++i) {
			if (categoricalCriteria[i].getNumInstances() == 0) {
				categoricalSplitPoint[i] = -1;
				continue;
			}

			categoricalSplitPoint[i] = 0;

			left.add(categoricalCriteria[i]);
			right.subtract(categoricalCriteria[i]);

			int leftNumInstances = left.getNumInstances() + missing.getNumInstances();
			int rightNumInstances = right.getNumInstances() + missing.getNumInstances();
			int totalNumInstances = total.getNumInstances() + missing.getNumInstances();

			if (left.getNumInstances() == 0
				|| right.getNumInstances() == 0
				|| minSamplesPerLeaf > leftNumInstances
				|| minSamplesPerLeaf > rightNumInstances
				|| minSampleRatioPerChild > ((double) leftNumInstances / (double) totalNumInstances)
				|| minSampleRatioPerChild > ((double) rightNumInstances / (double) totalNumInstances)
			) {
				continue;
			}

			double gain = total.gain(left, right);

			if (gain > bestGain && gain >= minInfoGain) {
				bestGain = gain;
				canSplit = true;
				splitPoint = categoricalSplitPoint.clone();
				best[0] = left.clone();
				best[1] = right.clone();
			}
		}

		return bestGain;
	}

	private double bestSplitCart() {
		int categoricalSize = featureMeta.getNumCategorical();

		bestGain = Criteria.INVALID_GAIN;
		splitPoint = new int[categoricalSize];
		best = new Criteria[2];
		int[] categoricalSplitPoint = new int[categoricalSize];
		int n = 1 << (categoricalSize - 1);

		Criteria left = criteriaOf();
		Criteria right = criteriaOf();

		for (int i = 1; i < n; ++i) {
			left.reset();
			right.reset();

			for (int j = 0; j < categoricalSize; ++j) {
				categoricalSplitPoint[j] = -1;

				if (categoricalCriteria[j].getNumInstances() == 0) {
					continue;
				}

				if ((i & (1 << j)) != 0) {
					categoricalSplitPoint[j] = 0;
					left.add(categoricalCriteria[j]);
				} else {
					categoricalSplitPoint[j] = 1;
					right.add(categoricalCriteria[j]);
				}
			}

			int leftNumInstances = left.getNumInstances() + missing.getNumInstances();
			int rightNumInstances = right.getNumInstances() + missing.getNumInstances();
			int totalNumInstances = total.getNumInstances() + missing.getNumInstances();

			if (left.getNumInstances() == 0
				|| right.getNumInstances() == 0
				|| minSamplesPerLeaf > leftNumInstances
				|| minSamplesPerLeaf > rightNumInstances
				|| minSampleRatioPerChild > ((double) leftNumInstances / (double) totalNumInstances)
				|| minSampleRatioPerChild > ((double) rightNumInstances / (double) totalNumInstances)
			) {
				continue;
			}

			double gain = total.gain(left, right);

			if (gain > bestGain && gain >= minInfoGain) {
				bestGain = gain;
				canSplit = true;
				splitPoint = categoricalSplitPoint.clone();
				best[0] = left.clone();
				best[1] = right.clone();
			}
		}

		return bestGain;
	}

	private double bestSplitCart(int curLeafCount) {
		if (curLeafCount + 2 >= maxLeaves) {
			return Criteria.INVALID_GAIN;
		}

		count();

		// if all instances are missing, it prune the tree.
		if (missing.getNumInstances() == partition.dataIndices.size()) {
			return Criteria.INVALID_GAIN;
		}

		if (params.get(Criteria.Gain.GAIN) == Criteria.Gain.GINI
			&& data.labelMeta.getNumCategorical() == 2) {
             return bestSplitCartIncrement();
		} else {
			return bestSplitCart();
		}
	}

	private double bestSplitInfo(int curLeafCount) {
		count();
		// if all instances are missing, it prune the tree.
		if (missing.getNumInstances() == partition.dataIndices.size()) {
			return Criteria.INVALID_GAIN;
		}

		int categoricalSize = featureMeta.getNumCategorical();
		int zeroSize = 0;

		for (int i = 0; i < categoricalSize; ++i) {
			if (categoricalCriteria[i].getNumInstances() == 0
				|| categoricalCriteria[i].getWeightSum() == 0.) {
				zeroSize += 1;
				continue;
			}

			if (minSamplesPerLeaf > categoricalCriteria[i].getNumInstances() + missing.getNumInstances()
				|| minSampleRatioPerChild >
				(
					((double) categoricalCriteria[i].getNumInstances() + (double) missing.getNumInstances())
						/ ((double) total.getNumInstances() + (double) missing.getNumInstances())
				)) {
				return Criteria.INVALID_GAIN;
			}
		}

		int sizeChildren = categoricalSize - zeroSize;
		if (sizeChildren < 2
			|| curLeafCount + sizeChildren >= maxLeaves) {
			return Criteria.INVALID_GAIN;
		}

		splitPoint = new int[categoricalSize];
		best = new Criteria[sizeChildren];
		zeroSize = 0;
		for (int i = 0; i < categoricalSize; ++i) {
			splitPoint[i] = -1;
			if (categoricalCriteria[i].getNumInstances() == 0
				|| categoricalCriteria[i].getWeightSum() == 0.) {
				zeroSize += 1;
				continue;
			}

			int index = i - zeroSize;
			splitPoint[i] = index;
			best[index] = categoricalCriteria[i];
		}

		bestGain = total.gain(best);

		if (bestGain <= Criteria.INVALID_GAIN || bestGain < minInfoGain) {
			return Criteria.INVALID_GAIN;
		}

		canSplit = true;

		return bestGain;
	}
}
