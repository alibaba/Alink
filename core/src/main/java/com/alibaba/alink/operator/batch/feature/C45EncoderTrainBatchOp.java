package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.C45TrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("C45决策树分类编码器训练")
@NameEn(" C45 Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.C45Encoder")
public class C45EncoderTrainBatchOp extends C45TrainBatchOp {
	public C45EncoderTrainBatchOp(Params params) {
		super(params);
	}
}
