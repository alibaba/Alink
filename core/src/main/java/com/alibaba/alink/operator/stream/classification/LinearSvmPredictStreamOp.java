package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * Linear svm predict stream operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性支持向量机预测")
@NameEn("Linear SVM Prediction")
public final class LinearSvmPredictStreamOp extends ModelMapStreamOp <LinearSvmPredictStreamOp>
	implements LinearSvmPredictParams <LinearSvmPredictStreamOp> {

	private static final long serialVersionUID = 5279655343341273024L;

	public LinearSvmPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LinearSvmPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LinearSvmPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LinearSvmPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}
}
