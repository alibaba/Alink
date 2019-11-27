package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;

/**
 * Bisecting KMeans prediction based on the model fitted by BisectingKMeansTrainBatchOp.
 */
public final class BisectingKMeansPredictStreamOp extends ModelMapStreamOp <BisectingKMeansPredictStreamOp>
	implements BisectingKMeansPredictParams <BisectingKMeansPredictStreamOp> {

	public BisectingKMeansPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public BisectingKMeansPredictStreamOp(BatchOperator model, Params params) {
		super(model, BisectingKMeansModelMapper::new, params);
	}
}
