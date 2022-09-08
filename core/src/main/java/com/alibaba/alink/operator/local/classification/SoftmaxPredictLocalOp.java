package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

/**
 * Softmax predict local operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Softmax预测")
@NameEn("Softmax Prediction")
public final class SoftmaxPredictLocalOp extends ModelMapLocalOp <SoftmaxPredictLocalOp>
	implements SoftmaxPredictParams <SoftmaxPredictLocalOp> {

	public SoftmaxPredictLocalOp() {
		this(new Params());
	}

	public SoftmaxPredictLocalOp(Params params) {
		super(SoftmaxModelMapper::new, params);
	}

}
