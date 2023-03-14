package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.ExclusiveFeatureBundleModelMapper;
import com.alibaba.alink.params.feature.ExclusiveFeatureBundlePredictParams;

@NameCn("互斥特征捆绑模型预测")
@NameEn("Exclusive Feature Bundle Prediction")
public class ExclusiveFeatureBundlePredictBatchOp extends ModelMapBatchOp <ExclusiveFeatureBundlePredictBatchOp>
	implements ExclusiveFeatureBundlePredictParams <ExclusiveFeatureBundlePredictBatchOp> {

	public ExclusiveFeatureBundlePredictBatchOp() {
		this(null);
	}

	public ExclusiveFeatureBundlePredictBatchOp(Params params) {
		super(ExclusiveFeatureBundleModelMapper::new, params);
	}
}
