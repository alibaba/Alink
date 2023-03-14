package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.CartTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("CART决策树分类编码器训练")
@NameEn(" Cart Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CartEncoder")
public class CartEncoderTrainBatchOp extends CartTrainBatchOp {
	public CartEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
