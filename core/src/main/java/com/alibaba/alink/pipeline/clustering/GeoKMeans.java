package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.GeoKMeansTrainBatchOp;
import com.alibaba.alink.params.clustering.GeoKMeansPredictParams;
import com.alibaba.alink.params.clustering.GeoKMeansTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * This version of kmeans support haversine distance, which is used to calculate the great-circle distance.
 * <p>
 * (https://en.wikipedia.org/wiki/Haversine_formula)
 */
public class GeoKMeans extends Trainer <GeoKMeans, KMeansModel> implements
	GeoKMeansTrainParams <GeoKMeans>,
	GeoKMeansPredictParams <GeoKMeans> {

	private static final long serialVersionUID = -6254559043009906583L;

	public GeoKMeans() {
		super();
	}

	public GeoKMeans(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new GeoKMeansTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
