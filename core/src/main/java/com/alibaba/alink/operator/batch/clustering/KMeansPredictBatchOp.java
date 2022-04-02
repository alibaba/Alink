package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.params.clustering.KMeansPredictParams;

/**
 * KMeans prediction based on the model fitted by KMeansTrainBatchOp.
 */
@NameCn("K均值聚类预测")
public final class KMeansPredictBatchOp extends ModelMapBatchOp <KMeansPredictBatchOp>
	implements KMeansPredictParams <KMeansPredictBatchOp> {
	private static final long serialVersionUID = -4673084154965905629L;

	/**
	 * null constructor
	 */
	public KMeansPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor
	 *
	 * @param params
	 */
	public KMeansPredictBatchOp(Params params) {
		super(KMeansModelMapper::new, params);
	}
}
