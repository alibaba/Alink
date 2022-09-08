package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LinearRegPredictParams;

/**
 * Linear regression predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性回归预测")
@NameEn("Linear Regression Prediction")
public final class LinearRegPredictBatchOp extends ModelMapBatchOp <LinearRegPredictBatchOp>
	implements LinearRegPredictParams <LinearRegPredictBatchOp> {

	private static final long serialVersionUID = 1539909945152583467L;

	public LinearRegPredictBatchOp() {
		this(new Params());
	}

	public LinearRegPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
