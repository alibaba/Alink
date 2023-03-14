package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;
import com.alibaba.alink.params.clustering.BisectingKMeansTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Bisecting k-means is a kind of hierarchical clustering algorithm.
 * <p>
 * Bisecting k-means algorithm starts from a single cluster that contains all points.
 * Iteratively it finds divisible clusters on the bottom level and bisects each of them using
 * k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
 *
 * @see <a href="http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf">
 * Steinbach, Karypis, and Kumar, A comparison of document clustering techniques,
 * KDD Workshop on Text Mining, 2000.</a>
 */
@NameCn("二分K均值聚类")
public class BisectingKMeans extends Trainer <BisectingKMeans, BisectingKMeansModel> implements
	BisectingKMeansTrainParams <BisectingKMeans>,
	BisectingKMeansPredictParams <BisectingKMeans>,
	HasLazyPrintModelInfo <BisectingKMeans> {

	private static final long serialVersionUID = -6873946218553613224L;

	public BisectingKMeans() {
		super();
	}

	public BisectingKMeans(Params params) {
		super(params);
	}

}
