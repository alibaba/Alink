package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("决策树回归编码器训练")
@NameEn("Decision Tree RegEncoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.DecisionTreeRegEncoder")
public class DecisionTreeRegEncoderTrainBatchOp extends DecisionTreeRegTrainBatchOp {
	public DecisionTreeRegEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
