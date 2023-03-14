package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.Id3TrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("ID3决策树分类编码器训练")
@NameEn(" Id3 Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.Id3Encoder")
public class Id3EncoderTrainBatchOp extends Id3TrainBatchOp {
	public Id3EncoderTrainBatchOp(Params params) {
		super(params);
	}
}
