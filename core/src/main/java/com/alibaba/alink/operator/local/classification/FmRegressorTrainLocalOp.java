package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.fm.FmRegressorModelInfo;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 *
 */
@NameCn("Local Fm 回归训练")
@NameEn("Local Fm regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.FmRegressor")
public final class FmRegressorTrainLocalOp extends FmTrainLocalOp <FmRegressorTrainLocalOp>
	implements FmTrainParams <FmRegressorTrainLocalOp>,
WithModelInfoLocalOp <FmRegressorModelInfo, FmRegressorTrainLocalOp, FmRegressorModelInfoLocalOp> {

	public FmRegressorTrainLocalOp() {
		super(new Params(), Task.REGRESSION);
	}

	public FmRegressorTrainLocalOp(Params params) {
		super(params, Task.REGRESSION);
	}

	@Override
	public FmRegressorModelInfoLocalOp getModelInfoLocalOp() {
		return new FmRegressorModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
