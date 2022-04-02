package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;

/**
 * Linear svm predict batch operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性支持向量机预测")
public final class LinearSvmPredictBatchOp extends ModelMapBatchOp <LinearSvmPredictBatchOp>
	implements LinearSvmPredictParams <LinearSvmPredictBatchOp> {

	private static final long serialVersionUID = 238159630290445407L;

	public LinearSvmPredictBatchOp() {
		this(new Params());
	}

	public LinearSvmPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}

}
