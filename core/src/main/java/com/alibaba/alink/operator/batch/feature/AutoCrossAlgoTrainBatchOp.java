package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("AutoCross训练")
@NameEn("AutoCross Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.AutoCrossAlgo")
public class AutoCrossAlgoTrainBatchOp extends AutoCrossTrainBatchOp {
	public AutoCrossAlgoTrainBatchOp(Params params) {
		super(params);
	}
}
