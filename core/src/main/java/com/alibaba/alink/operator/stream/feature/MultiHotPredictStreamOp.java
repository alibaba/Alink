package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.MultiHotModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.MultiHotPredictParams;

/**
 *  Multi hot encoding predict process.
 */
public class MultiHotPredictStreamOp extends ModelMapStreamOp <MultiHotPredictStreamOp>
	implements MultiHotPredictParams <MultiHotPredictStreamOp> {

	private static final long serialVersionUID = 3986423530880867993L;

	/**
	 * constructor.
	 *
	 * @param model the model.
	 */
	public MultiHotPredictStreamOp(BatchOperator model) {
		super(model, MultiHotModelMapper::new, new Params());
	}

	/**
	 * @param model  the model.
	 * @param params the parameter set.
	 */
	public MultiHotPredictStreamOp(BatchOperator model, Params params) {
		super(model, MultiHotModelMapper::new, params);
	}
}
