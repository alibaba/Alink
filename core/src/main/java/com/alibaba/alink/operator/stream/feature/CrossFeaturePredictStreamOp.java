package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.CrossFeatureModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;

/**
 * Cross selected columns to build new vector type data.
 */
@NameCn("Cross特征预测")
@NameEn("Cross feature prediction")
public class CrossFeaturePredictStreamOp extends ModelMapStreamOp<CrossFeaturePredictStreamOp>
	implements CrossFeaturePredictParams <CrossFeaturePredictStreamOp> {

	public CrossFeaturePredictStreamOp() {
		super(CrossFeatureModelMapper::new, new Params());
	}

	public CrossFeaturePredictStreamOp(Params params) {
		super(CrossFeatureModelMapper::new, params);
	}

	public CrossFeaturePredictStreamOp(BatchOperator model) {
		super(model, CrossFeatureModelMapper::new, new Params());
	}


	public CrossFeaturePredictStreamOp(BatchOperator model, Params params) {
		super(model, CrossFeatureModelMapper::new, params);
	}
}
