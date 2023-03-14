package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LinearSvrPredictParams;

/**
 * *
 *
 * @author weibo zhao
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("线性SVR预测")
@NameEn("Linear SVR  Prediction")
public final class LinearSvrPredictBatchOp extends ModelMapBatchOp <LinearSvrPredictBatchOp>
	implements LinearSvrPredictParams <LinearSvrPredictBatchOp> {

	private static final long serialVersionUID = 4438160354556417595L;

	public LinearSvrPredictBatchOp() {
		this(new Params());
	}

	public LinearSvrPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
