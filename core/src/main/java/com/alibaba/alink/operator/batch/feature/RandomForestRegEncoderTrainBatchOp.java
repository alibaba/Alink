package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("随机森林回归编码器训练")
@NameEn("Random Forest RegEncoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.RandomForestRegEncoder")
public class RandomForestRegEncoderTrainBatchOp extends RandomForestRegTrainBatchOp {
	public RandomForestRegEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
