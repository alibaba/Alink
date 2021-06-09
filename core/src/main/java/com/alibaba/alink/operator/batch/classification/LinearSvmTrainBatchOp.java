package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;

/**
 * Linear svm train batch operator. it uses hinge loss func by setting LinearModelType = SVM and model name = "linear
 * SVM".
 */
public final class LinearSvmTrainBatchOp extends BaseLinearModelTrainBatchOp <LinearSvmTrainBatchOp>
	implements LinearBinaryClassTrainParams <LinearSvmTrainBatchOp>,
	WithModelInfoBatchOp <LinearClassifierModelInfo, LinearSvmTrainBatchOp, LinearSvmModelInfoBatchOp> {

	private static final long serialVersionUID = -4642565621373421838L;

	public LinearSvmTrainBatchOp() {
		this(new Params());
	}

	public LinearSvmTrainBatchOp(Params params) {
		super(params, LinearModelType.SVM, "Linear SVM");
	}

	@Override
	public LinearSvmModelInfoBatchOp getModelInfoBatchOp() {
		return new LinearSvmModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
