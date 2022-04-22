package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelMapper;
import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.GlmPredictParams;

/**
 * Generalized Linear Model stream predict. https://en.wikipedia.org/wiki/Generalized_linear_model.
 */
@NameCn("广义线性回归预测")
public class GlmPredictStreamOp extends ModelMapStreamOp <GlmPredictStreamOp>
	implements GlmPredictParams <GlmPredictStreamOp> {

	private static final long serialVersionUID = -5222784580816513033L;

	public GlmPredictStreamOp() {
		super(GlmModelMapper::new, new Params());
	}

	public GlmPredictStreamOp(Params params) {
		super(GlmModelMapper::new, params);
	}

	public GlmPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public GlmPredictStreamOp(BatchOperator model, Params params) {
		super(model, GlmModelMapper::new, params);
	}

}
