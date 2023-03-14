package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.AutoCross.CrossCandidateSelectorModelMapper;
import com.alibaba.alink.params.feature.featuregenerator.CrossCandidateSelectorPredictParams;

@NameCn("cross候选特征选择预测")
@NameEn("Cross Candidate Selector Prediction")
public class CrossCandidateSelectorPredictBatchOp
	extends ModelMapBatchOp <CrossCandidateSelectorPredictBatchOp>
	implements CrossCandidateSelectorPredictParams <CrossCandidateSelectorPredictBatchOp> {

	private static final long serialVersionUID = 3987270029076248190L;

	public CrossCandidateSelectorPredictBatchOp() {
		this(new Params());
	}

	public CrossCandidateSelectorPredictBatchOp(Params params) {
		super(CrossCandidateSelectorModelMapper::new, params);
	}
}
