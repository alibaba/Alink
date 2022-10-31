package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.regression.LassoRegPredictParams;

/**
 * Lasso regression predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Lasso回归预测")
@NameEn("Lasso Regression Prediction")
public final class LassoRegPredictLocalOp extends ModelMapLocalOp <LassoRegPredictLocalOp>
	implements LassoRegPredictParams <LassoRegPredictLocalOp> {

	public LassoRegPredictLocalOp() {
		this(new Params());
	}

	public LassoRegPredictLocalOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
