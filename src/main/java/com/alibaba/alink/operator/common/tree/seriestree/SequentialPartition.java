package com.alibaba.alink.operator.common.tree.seriestree;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * SequentialPartition.
 */
class SequentialPartition {
	List<Tuple2<Integer, Double>> dataIndices;

	SequentialPartition(
		List<Tuple2<Integer, Double>> dataIndices) {
		this.dataIndices = dataIndices;
	}

	void sort(double[] featureValues) {
		ContinuousFeatureComparator comparator
			= new ContinuousFeatureComparator(featureValues);

		dataIndices.sort(comparator);
	}

	SequentialPartition[] splitContinuous(
		double[] featureValues, double splitPoint, double leftRatio, double rightRatio) {
		SequentialPartition left = new SequentialPartition(new ArrayList<>());
		SequentialPartition right = new SequentialPartition(new ArrayList<>());

		for (Tuple2<Integer, Double> index : dataIndices) {
			if (DenseData.isContinuousMissValue(featureValues[index.f0])) {
				left.dataIndices.add(Tuple2.of(index.f0, index.f1 * leftRatio));
				right.dataIndices.add(Tuple2.of(index.f0, index.f1 * rightRatio));
				continue;
			}

			if (featureValues[index.f0] <= splitPoint) {
				left.dataIndices.add(index);
			} else {
				right.dataIndices.add(index);
			}
		}

		return new SequentialPartition[] {left, right};
	}

	SequentialPartition[] splitCategorical(
		int[] featureValues, int sizeOfChildren, int[] splitPoint, double[] ratioOfChildren) {
		SequentialPartition[] children = new SequentialPartition[sizeOfChildren];

		for (int i = 0; i < sizeOfChildren; ++i) {
			children[i] = new SequentialPartition(new ArrayList<>());
		}

		for (Tuple2<Integer, Double> index : dataIndices) {
			if (DenseData.isCategoricalMissValue(featureValues[index.f0])) {
				for (int i = 0; i < sizeOfChildren; ++i) {
					children[i].dataIndices.add(Tuple2.of(index.f0, index.f1 * ratioOfChildren[i]));
				}

				continue;
			}

			children[splitPoint[featureValues[index.f0]]].dataIndices.add(index);
		}

		return children;
	}

	private static class ContinuousFeatureComparator implements Comparator<Tuple2<Integer, Double>> {
		private double[] featureValues;

		public ContinuousFeatureComparator(double[] featureValues) {
			this.featureValues = featureValues;
		}

		@Override
		public int compare(Tuple2<Integer, Double> left, Tuple2<Integer, Double> right) {
			return Double.compare(featureValues[left.f0], featureValues[right.f0]);
		}
	}
}
