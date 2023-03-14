package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("GBDT回归编码器训练")
@NameEn("Gbdt RegEncoder Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.GbdtRegEncoder")
public class GbdtRegEncoderTrainBatchOp extends GbdtRegTrainBatchOp {
	public GbdtRegEncoderTrainBatchOp(Params params) {
		super(params);
	}
}
