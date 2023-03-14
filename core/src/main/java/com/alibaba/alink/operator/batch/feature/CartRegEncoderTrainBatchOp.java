package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("CART决策树回归编码器训练")
@NameEn(" Cart RegEncoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CartRegEncoder")
public class CartRegEncoderTrainBatchOp extends CartRegTrainBatchOp {
	public CartRegEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
