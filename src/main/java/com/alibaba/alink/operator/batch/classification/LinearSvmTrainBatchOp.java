package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;
import com.alibaba.alink.params.classification.LinearSvmTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Linear svm train batch operator. it uses hinge loss func by setting LinearModelType = SVM and model name = "linear
 * SVM".
 *
 */
public final class LinearSvmTrainBatchOp extends BaseLinearModelTrainBatchOp<LinearSvmTrainBatchOp>
	implements LinearSvmTrainParams <LinearSvmTrainBatchOp> {

	public LinearSvmTrainBatchOp() {
		this(new Params());
	}

	public LinearSvmTrainBatchOp(Params params) {
		super(params.set(LinearBinaryClassTrainParams.L_2, 1.0 / params.get(LinearSvmTrainParams.C)),
			LinearModelType.SVM, "Linear SVM");
	}
}
