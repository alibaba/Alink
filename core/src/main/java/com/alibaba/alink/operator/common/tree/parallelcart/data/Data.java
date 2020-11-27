package com.alibaba.alink.operator.common.tree.parallelcart.data;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.EpsilonApproQuantile;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface Data {
	int getM();

	int getN();

	double[] getLabels();

	double[] getWeights();

	void loadFromRow(List <Row> rawData);

	void loadFromRowWithContinues(List <Row> rawData);

	boolean isRanking();

	int[] getQueryIdOffset();

	int getMaxQuerySize();

	FeatureMeta[] getFeatureMetas();

	int splitInstances(
		Node node,
		EpsilonApproQuantile.WQSummary summary,
		int[] indices,
		Slice slice
	);

	void constructHistogram(
		final boolean useInstanceCount,
		final int nodeSize,
		final int validInstanceCount,
		BitSet featureValid,
		int[] nodeIdCache,
		int[] validFeatureOffset,
		int[] aligned,
		double[] gradients,
		double[] hessions,
		double[] weights,
		ExecutorService executorService,
		Future <?>[] futures,
		double[] featureSplitHistogram
	);

	void sort();

	EpsilonApproQuantile.SketchEntry[] createWQSummary(
		int maxSize, double eps, EpsilonApproQuantile.SketchEntry[] buffer,
		double[] dynamicWeights, BitSet validFlags
	);

	void constructHistogramWithWQSummary(
		final boolean useInstanceCount,
		final int nodeSize,
		BitSet featureValid,
		int[] nodeIdCache,
		int[] validFeatureOffset,
		double[] gradients,
		double[] hessions,
		double[] weights,
		EpsilonApproQuantile.WQSummary[] summaries,
		ExecutorService executorService,
		Future <?>[] futures,
		double[] featureSplitHistogram
	);
}
