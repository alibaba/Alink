package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;

/**
 * Logistic regression predict local operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("逻辑回归预测")
@NameEn("Logistic Regression Prediction")
public final class LogisticRegressionPredictLocalOp extends ModelMapLocalOp <LogisticRegressionPredictLocalOp>
	implements LogisticRegressionPredictParams <LogisticRegressionPredictLocalOp> {

	public LogisticRegressionPredictLocalOp() {
		this(new Params());
	}

	public LogisticRegressionPredictLocalOp(Params params) {
		super(LinearModelMapper::new, params);
	}

}
