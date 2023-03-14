package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.params.clustering.BisectingKMeansPredictParams;

/**
 * Bisecting KMeans prediction based on the model fitted by BisectingKMeansTrainBatchOp.
 */
@NameCn("二分K均值聚类预测")
@NameEn("Bisecting KMeans Prediction")
public final class BisectingKMeansPredictBatchOp extends ModelMapBatchOp <BisectingKMeansPredictBatchOp>
	implements BisectingKMeansPredictParams <BisectingKMeansPredictBatchOp> {

	private static final long serialVersionUID = 835036024472438981L;

	public BisectingKMeansPredictBatchOp() {
		this(null);
	}

	public BisectingKMeansPredictBatchOp(Params params) {
		super(BisectingKMeansModelMapper::new, params);
	}
}
