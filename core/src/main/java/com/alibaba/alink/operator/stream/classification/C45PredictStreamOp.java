package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.C45PredictParams;

/**
 * The stream operator that predict the data using the c45 model.
 */
@NameCn("C45决策树分类预测")
public final class C45PredictStreamOp extends ModelMapStreamOp <C45PredictStreamOp>
	implements C45PredictParams <C45PredictStreamOp> {
	private static final long serialVersionUID = -520190769859999252L;

	public C45PredictStreamOp() {
		super(RandomForestModelMapper::new, new Params());
	}

	public C45PredictStreamOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

	public C45PredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public C45PredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
