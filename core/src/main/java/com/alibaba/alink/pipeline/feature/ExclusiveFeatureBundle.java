package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundleTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("互斥特征捆绑")
@NameEn("Exclusive Feature Bundle")
public class ExclusiveFeatureBundle extends Trainer <ExclusiveFeatureBundle, ExclusiveFeatureBundleModel>
	implements ExclusiveFeatureBundleTrainParams <ExclusiveFeatureBundle>,
	ExclusiveFeatureBundlePredictParams <ExclusiveFeatureBundle> {

	public ExclusiveFeatureBundle() {this(null);}

	public ExclusiveFeatureBundle(Params params) {
		super(params);
	}
}
