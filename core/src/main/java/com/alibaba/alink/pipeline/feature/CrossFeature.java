package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;
import com.alibaba.alink.params.feature.CrossFeatureTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Cross特征预测")
public class CrossFeature extends Trainer <CrossFeature, CrossFeatureModel> implements
	CrossFeatureTrainParams <CrossFeature>,
	CrossFeaturePredictParams <CrossFeature> {

	private static final long serialVersionUID = -8593371764511217184L;

	public CrossFeature() {
	}

	public CrossFeature(Params params) {
		super(params);
	}

}
