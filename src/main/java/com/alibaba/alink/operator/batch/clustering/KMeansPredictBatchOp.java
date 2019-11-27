package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.clustering.KMeansPredictParams;

/**
 * Find  the closest cluster center for every point.
 */
public final class KMeansPredictBatchOp extends ModelMapBatchOp <KMeansPredictBatchOp>
	implements KMeansPredictParams <KMeansPredictBatchOp> {
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
