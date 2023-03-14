package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("随机森林编码器训练")
@NameEn("Random Forest Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.RandomForestEncoder")
public class RandomForestEncoderTrainBatchOp extends RandomForestTrainBatchOp {
	public RandomForestEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
