package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;

/**
 * Ridge regression predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("岭回归预测")
@NameEn("Ridge Regression Prediction")
public final class RidgeRegPredictLocalOp extends ModelMapLocalOp <RidgeRegPredictLocalOp>
	implements RidgeRegPredictParams <RidgeRegPredictLocalOp> {

	public RidgeRegPredictLocalOp() {
		super(LinearModelMapper::new, new Params());
	}

	public RidgeRegPredictLocalOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
