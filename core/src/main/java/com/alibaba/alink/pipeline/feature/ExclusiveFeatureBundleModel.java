package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelMapper;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("互斥特征捆绑模型")
@NameEn("Exclusive Feature Bundle")
public class ExclusiveFeatureBundleModel extends MapModel <ExclusiveFeatureBundleModel>
	implements ExclusiveFeatureBundlePredictParams <ExclusiveFeatureBundleModel> {

	public ExclusiveFeatureBundleModel() {
		this(null);
	}

	public ExclusiveFeatureBundleModel(Params params) {
		super(ExclusiveFeatureBundleModelMapper::new, params);
	}
}
