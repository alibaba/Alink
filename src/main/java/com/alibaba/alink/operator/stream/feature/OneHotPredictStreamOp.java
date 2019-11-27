package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;

/**
 * *
 * A one-hot stream operator that maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 *
 */
public final class OneHotPredictStreamOp extends ModelMapStreamOp <OneHotPredictStreamOp>
	implements OneHotPredictParams <OneHotPredictStreamOp> {

	/**
	 * constructor.
	 *
	 * @param model the model.
	 */
	public OneHotPredictStreamOp(BatchOperator model) {
		super(model, OneHotModelMapper::new, new Params());
	}

	/**
	 * @param model  the model.
	 * @param params the parameter set.
	 */
	public OneHotPredictStreamOp(BatchOperator model, Params params) {
		super(model, OneHotModelMapper::new, params);
	}
}
