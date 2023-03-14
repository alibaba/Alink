package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.C45PredictParams;

/**
 * The batch operator that predict the data using the c45 model.
 */
@NameCn("C45决策树分类预测")
@NameEn("C45 Decision Tree Prediction")
public final class C45PredictBatchOp extends ModelMapBatchOp <C45PredictBatchOp> implements
	C45PredictParams <C45PredictBatchOp> {
	private static final long serialVersionUID = -3642003580227332493L;

	public C45PredictBatchOp() {
		this(null);
	}

	public C45PredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
