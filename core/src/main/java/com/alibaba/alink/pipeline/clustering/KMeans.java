package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.params.clustering.KMeansPredictParams;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 * analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 * observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 * <p>
 * (https://en.wikipedia.org/wiki/K-means_clustering)
 */
public class KMeans extends Trainer <KMeans, KMeansModel> implements
	KMeansTrainParams <KMeans>,
	KMeansPredictParams <KMeans> {

	public KMeans() {
		super();
	}

	public KMeans(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new KMeansTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
