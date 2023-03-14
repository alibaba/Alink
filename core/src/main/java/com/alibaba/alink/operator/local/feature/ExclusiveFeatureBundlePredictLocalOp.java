package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;

@NameCn("互斥特征捆绑模型预测")
public class ExclusiveFeatureBundlePredictLocalOp extends ModelMapLocalOp <ExclusiveFeatureBundlePredictLocalOp>
	implements ExclusiveFeatureBundlePredictParams <ExclusiveFeatureBundlePredictLocalOp> {

	public ExclusiveFeatureBundlePredictLocalOp() {
		this(null);
	}

	public ExclusiveFeatureBundlePredictLocalOp(Params params) {
		super(ExclusiveFeatureBundleModelMapper::new, params);
	}
}
