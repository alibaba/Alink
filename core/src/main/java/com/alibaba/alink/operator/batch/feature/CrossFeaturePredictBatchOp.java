package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.CrossFeatureModelMapper;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;

/**
 * Cross selected columns to build new vector type data.
 */
@NameCn("Cross特征预测")
public class CrossFeaturePredictBatchOp extends ModelMapBatchOp<CrossFeaturePredictBatchOp>
	implements CrossFeaturePredictParams <CrossFeaturePredictBatchOp> {

	public CrossFeaturePredictBatchOp() {
		this(new Params());
	}

	public CrossFeaturePredictBatchOp(Params params) {
		super(CrossFeatureModelMapper::new, params);
	}
}
