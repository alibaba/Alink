package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.KMeansPredictParams;

/**
 * Find  the closest cluster center for every point.
 */
public final class KMeansPredictStreamOp extends ModelMapStreamOp <KMeansPredictStreamOp>
	implements KMeansPredictParams <KMeansPredictStreamOp> {

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
