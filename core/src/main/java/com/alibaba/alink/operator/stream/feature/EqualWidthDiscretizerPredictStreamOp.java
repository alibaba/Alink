package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public class EqualWidthDiscretizerPredictStreamOp extends ModelMapStreamOp <EqualWidthDiscretizerPredictStreamOp>
	implements QuantileDiscretizerPredictParams <EqualWidthDiscretizerPredictStreamOp> {

	private static final long serialVersionUID = -4788685477954335971L;

	public EqualWidthDiscretizerPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public EqualWidthDiscretizerPredictStreamOp(BatchOperator model, Params params) {
		super(model, QuantileDiscretizerModelMapper::new, params);
	}
}
