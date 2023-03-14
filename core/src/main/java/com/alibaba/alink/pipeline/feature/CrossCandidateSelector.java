package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.CrossCandidateSelectorTrainParams;
import com.alibaba.alink.params.feature.featuregenerator.CrossCandidateSelectorPredictParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Cross候选特征筛选模型")
public class CrossCandidateSelector extends
	Trainer <CrossCandidateSelector, CrossCandidateSelectorModel> implements
	CrossCandidateSelectorTrainParams <CrossCandidateSelector>,
	CrossCandidateSelectorPredictParams <CrossCandidateSelector> {

	private static final long serialVersionUID = -4475238813305040400L;

	public CrossCandidateSelector() {
	}

	public CrossCandidateSelector(Params params) {
		super(params);
	}

}
