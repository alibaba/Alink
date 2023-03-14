package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.AutoCross.CrossCandidateSelectorModelMapper;
import com.alibaba.alink.params.feature.featuregenerator.CrossCandidateSelectorPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("Cross候选特征筛选模型")
public class CrossCandidateSelectorModel extends MapModel <CrossCandidateSelectorModel>
	implements CrossCandidateSelectorPredictParams <CrossCandidateSelectorModel> {

	public CrossCandidateSelectorModel() {
		this(new Params());
	}

	public CrossCandidateSelectorModel(Params params) {
		super(CrossCandidateSelectorModelMapper::new, params);
	}
}
