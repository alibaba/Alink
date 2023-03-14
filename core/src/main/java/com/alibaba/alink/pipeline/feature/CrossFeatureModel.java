package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.CrossFeatureModelMapper;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("Cross特征预测")
public class CrossFeatureModel extends MapModel <CrossFeatureModel>
	implements CrossFeaturePredictParams <CrossFeatureModel> {

	public CrossFeatureModel() {
		this(new Params());
	}

	public CrossFeatureModel(Params params) {
		super(CrossFeatureModelMapper::new, params);
	}
}
