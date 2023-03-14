package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.KMeansPredictParams;

/**
 * KMeans prediction for streaming data based on the model fitted by KMeansTrainBatchOp.
 */
@NameCn("K均值聚类预测")
@NameEn("KMeans Prediction")
public final class KMeansPredictStreamOp extends ModelMapStreamOp <KMeansPredictStreamOp>
	implements KMeansPredictParams <KMeansPredictStreamOp> {

	private static final long serialVersionUID = -7696194188126101276L;

	public KMeansPredictStreamOp() {
		super(KMeansModelMapper::new, new Params());
	}

	public KMeansPredictStreamOp(Params params) {
		super(KMeansModelMapper::new, params);
	}

	/**
	 * default constructor
	 *
	 * @param model trained from kMeansBatchOp
	 */
	public KMeansPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public KMeansPredictStreamOp(BatchOperator model, Params params) {
		super(model, KMeansModelMapper::new, params);
	}
}
