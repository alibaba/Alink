package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * *
 *
 * @author yangxu
 */
@NameCn("线性SVR训练")
@NameEn("Linear SVR  Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.LinearSvr")
public final class LinearSvrTrainBatchOp extends BaseLinearModelTrainBatchOp <LinearSvrTrainBatchOp>
	implements LinearSvrTrainParams <LinearSvrTrainBatchOp> {

	private static final long serialVersionUID = 713597255264745408L;

	public LinearSvrTrainBatchOp() {
		this(new Params());
	}

	public LinearSvrTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Linear SVR");
	}

}
