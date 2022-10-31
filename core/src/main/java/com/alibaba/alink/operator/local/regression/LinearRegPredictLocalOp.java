package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;

/**
 * Linear regression predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性回归预测")
@NameEn("Linear Regression Prediction")
public final class LinearRegPredictLocalOp extends ModelMapLocalOp <LinearRegPredictLocalOp>
	implements LinearRegPredictParams <LinearRegPredictLocalOp> {

	public LinearRegPredictLocalOp() {
		this(new Params());
	}

	public LinearRegPredictLocalOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
