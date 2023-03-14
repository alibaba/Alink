package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("决策树编码器训练")
@NameEn("Decision Tree Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.DecisionTreeEncoder")
public class DecisionTreeEncoderTrainBatchOp extends DecisionTreeTrainBatchOp {
	public DecisionTreeEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
