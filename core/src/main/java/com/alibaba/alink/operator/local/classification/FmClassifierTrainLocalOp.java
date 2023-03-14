package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.fm.FmClassifierModelInfo;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 *
 */
@NameCn("Local Fm 分类训练")
@NameEn("Local Fm classification Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.FmClassifier")
public final class FmClassifierTrainLocalOp extends FmTrainLocalOp <FmClassifierTrainLocalOp>
	implements FmTrainParams <FmClassifierTrainLocalOp>,
WithModelInfoLocalOp <FmClassifierModelInfo, FmClassifierTrainLocalOp, FmClassifierModelInfoLocalOp> {

	public FmClassifierTrainLocalOp() {
		super(new Params(), Task.BINARY_CLASSIFICATION);
	}
	public FmClassifierTrainLocalOp(Params params) {
		super(params, Task.BINARY_CLASSIFICATION);
	}

	@Override
	public FmClassifierModelInfoLocalOp getModelInfoLocalOp() {
		return new FmClassifierModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
