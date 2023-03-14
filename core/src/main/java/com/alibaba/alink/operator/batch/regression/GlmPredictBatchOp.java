package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.params.regression.GlmPredictParams;

/**
 * Generalized Linear Model. https://en.wikipedia.org/wiki/Generalized_linear_model.
 */
@NameCn("广义线性回归预测")
@NameEn("GLM Prediction")
public class GlmPredictBatchOp extends ModelMapBatchOp <GlmPredictBatchOp>
	implements GlmPredictParams <GlmPredictBatchOp> {
	private static final long serialVersionUID = 1855615229106536018L;

	public GlmPredictBatchOp() {
		this(new Params());
	}

	public GlmPredictBatchOp(Params params) {
		super(GlmModelMapper::new, params);
	}
}
