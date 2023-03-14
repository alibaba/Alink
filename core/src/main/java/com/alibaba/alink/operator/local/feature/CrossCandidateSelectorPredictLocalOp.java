package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.AutoCross.CrossCandidateSelectorModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.feature.featuregenerator.CrossCandidateSelectorPredictParams;

@NameCn("cross候选特征选择预测")
@NameEn("Cross Candidate Selector Prediction")
public class CrossCandidateSelectorPredictLocalOp
	extends ModelMapLocalOp <CrossCandidateSelectorPredictLocalOp>
	implements CrossCandidateSelectorPredictParams <CrossCandidateSelectorPredictLocalOp> {

	public CrossCandidateSelectorPredictLocalOp() {
		this(new Params());
	}

	public CrossCandidateSelectorPredictLocalOp(Params params) {
		super(CrossCandidateSelectorModelMapper::new, params);
	}
}
