package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;

/**
 * Bisecting KMeans prediction based on the model fitted by BisectingKMeansTrainBatchOp.
 */
public final class BisectingKMeansPredictBatchOp extends ModelMapBatchOp <BisectingKMeansPredictBatchOp>
	implements BisectingKMeansPredictParams <BisectingKMeansPredictBatchOp> {

	public BisectingKMeansPredictBatchOp() {
		this(null);
	}

	public BisectingKMeansPredictBatchOp(Params params) {
		super(BisectingKMeansModelMapper::new, params);
	}
}
