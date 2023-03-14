package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("GBDT分类编码训练")
@NameEn("Gbdt Encoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.GbdtEncoder")
public class GbdtEncoderTrainBatchOp extends GbdtTrainBatchOp {
	public GbdtEncoderTrainBatchOp() {
		super(new Params());
	}

	public GbdtEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
