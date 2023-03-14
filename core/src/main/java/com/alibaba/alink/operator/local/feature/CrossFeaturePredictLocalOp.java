package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.CrossFeatureModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;

/**
 * Cross selected columns to build new vector type data.
 */
@NameCn("Cross特征预测")
@NameEn("Cross Feature Prediction")
public class CrossFeaturePredictLocalOp extends ModelMapLocalOp <CrossFeaturePredictLocalOp>
	implements CrossFeaturePredictParams <CrossFeaturePredictLocalOp> {

	public CrossFeaturePredictLocalOp() {
		this(new Params());
	}

	public CrossFeaturePredictLocalOp(Params params) {
		super(CrossFeatureModelMapper::new, params);
	}
}
