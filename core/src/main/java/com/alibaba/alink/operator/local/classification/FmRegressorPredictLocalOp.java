package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Local fm predict local operator. this operator predict data's label with fm model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Local FM回归预测")
@NameEn("Local FM Regressor Prediction")
public final class FmRegressorPredictLocalOp extends ModelMapLocalOp <FmRegressorPredictLocalOp>
	implements FmPredictParams <FmRegressorPredictLocalOp> {

	public FmRegressorPredictLocalOp() {
		this(new Params());
	}

	public FmRegressorPredictLocalOp(Params params) {
		super(FmModelMapper::new, params);
	}

}
