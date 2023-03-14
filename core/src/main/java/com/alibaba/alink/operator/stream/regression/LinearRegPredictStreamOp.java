package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * Linear regression predict stream operator. this operator predict data's regression value with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性回归预测")
@NameEn("Linear Regression Prediction")
public class LinearRegPredictStreamOp extends ModelMapStreamOp <LinearRegPredictStreamOp>
	implements LinearRegPredictParams <LinearRegPredictStreamOp> {

	private static final long serialVersionUID = -5279929521940820773L;

	public LinearRegPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LinearRegPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LinearRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public LinearRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
