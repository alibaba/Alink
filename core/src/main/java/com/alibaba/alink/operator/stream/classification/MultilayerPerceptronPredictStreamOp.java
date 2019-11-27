package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;

public final class MultilayerPerceptronPredictStreamOp
	extends ModelMapStreamOp <MultilayerPerceptronPredictStreamOp>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronPredictStreamOp> {

	public MultilayerPerceptronPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public MultilayerPerceptronPredictStreamOp(BatchOperator model, Params params) {
		super(model, MlpcModelMapper::new, params);
	}
}
